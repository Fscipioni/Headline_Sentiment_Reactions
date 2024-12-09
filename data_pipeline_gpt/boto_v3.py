"""Accessing the S3 bucket and folders.
Sorting and pairing the screenshots.
Calling the OCR API endpoints.
Compiling the data into a DataFrame.
Saving the DataFrame to a CSV file.

script job 
Connect to your S3 bucket.
Iterate over each news organization's folder.
For each folder:
List all images and sort them by timestamp.
Pair every two images (they correspond to the same article).
For each pair:
Download the images locally or read them directly.
Call the OCR API endpoints with the appropriate prompts.
Collect the extracted data.
Compile all data into a Pandas DataFrame with the specified headers.
Save the DataFrame to a CSV file.

"""

import boto3
import pandas as pd
import requests
import datetime
import re
import os
import tempfile
import json
import time
import logging
# Configure logging
logging.basicConfig(
    filename='processing_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# AWS S3 Configuration
s3 = boto3.client('s3')
bucket_name = 'nlp-project-screenshots'

# CSV Headers
headers = [
    'news_org', 'title', 'summary', 'total_reactions', 'article_screenshot_s3_link',
    'reactions_screenshot_s3_link', 'article_sc_date', 'reactions_sc_date',
    'total_comments', 'total_shares', 'total_like', 'total_love', 'total_haha',
    'total_wow', 'total_sad', 'total_angry', 'total_care', 'unrecognized_total', 'prompt_used_for_article_scrapping',
    'prompt_used_for_reactions_scrapping'
]

# OCR API Endpoint
OCR_API_URL = 'https://ocr-hcj9.onrender.com/api/ocr'

# Prompts
article_prompt = (
    "Extract the following information from the given screenshot and return the response in JSON format:\n"
    "'news_org': The name of the news organization that appears at the top of the screenshot.\n"
    "'title': The title of the news article as scraped from the screenshot. this text in Bold.\n"
    "'summary': The summary of the article, typically found at the top and of the image and below the organization name and date.\n"
    "'total_reactions': The total number of reactions shown in the post (likes, dislikes, etc.).\n"
    "'total_comments': The total number of comments displayed in the post.\n"
    "'total_shares': The total number of shares shown in the post.\n"
    "Please respond in JSON format."
)

reactions_prompt = (
    "Give me the count for each emoji reaction in JSON, the order of reactions is by their count and it starts horizontally, \
      and if there is the word 'More' they will continue being listed vertically under the word 'More'. Here's the list of possible emojis: \
    like (blue circle with thumbs up), love (red circle with a white shaped heart), haha (laughing emoji), \
    angry (angy emoji that is red),\
     wow (yellow emoji with 3 inner black circles),\
     care (it is the hug emoji, that is a yellow shaped with a red heart and two inner black circles), sad (yellow circle with a blue drop on the left). \
     unrecognized emoji count (this is the count of for emoji that you did not recognize from the above list)\
        Please respect the given order and give me the exact number for each and the name in JSON response should be the unique word of each emoji. Missing emojis from the image should return a total of Zero. Add an optional field for text in the background."
)

# Helper function to list all objects in a folder using pagination
def list_all_objects_in_folder(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    all_objects = []
    for page in page_iterator:
        all_objects.extend(page.get('Contents', []))
    return all_objects

# Initialize an empty list to store the data
data = []

# Step 1: List all folders (news organizations) in the bucket
response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
folders = [prefix.get('Prefix') for prefix in response.get('CommonPrefixes', [])]
total_folders = len(folders)

for idx, folder in enumerate(folders, start=1):
    news_org = folder.rstrip('/')
    message = f"Processing folder {idx}/{total_folders}: {news_org}"
    print(message)
    logging.info(message)
    start_time = time.time()

    # Step 2: List all images in the folder with pagination
    objects = list_all_objects_in_folder(bucket_name, folder)
    images = [obj['Key'] for obj in objects if obj['Key'].endswith('.png')]
    message = f"Found {len(images)} images in folder '{news_org}'."
    print(message)
    logging.info(message)

    def extract_timestamp(filename):
        # Regex pattern that matches both 12-hour with AM/PM and 24-hour time formats
        match = re.search(r'Screenshot (\d{4}-\d{2}-\d{2} at \d+\.\d+\.\d+)(?:[^\w]*([AP]M))?\.png', filename)
        if match:
            date_str = match.group(1)
            am_pm = match.group(2)  # This will be None if 24-hour format is used
            try:
                # Attempt parsing based on the presence of AM/PM
                if am_pm:
                    # 12-hour format with AM/PM
                    return datetime.datetime.strptime(f"{date_str} {am_pm}", '%Y-%m-%d at %I.%M.%S %p')
                else:
                    # 24-hour format
                    return datetime.datetime.strptime(date_str, '%Y-%m-%d at %H.%M.%S')
            except ValueError:
                pass
        return datetime.datetime.min  # Return minimal date if parsing fails


    images.sort(key=lambda x: extract_timestamp(x))

    # Step 4: Pair every two images
    for i in range(0, len(images), 2):
        if i+1 >= len(images):
            message = f"Unpaired image found in {news_org}: {images[i]}"
            print(message)
            logging.info(message)
            break  # Or handle the last unpaired image if necessary

        sc1_key = images[i]
        sc2_key = images[i+1]

        message = f"Processing pair:\n\tArticle Image: {sc1_key}\n\tReactions Image: {sc2_key}"
        print(message)
        logging.info(message)

        progress = (i+1) / (len(images)/2) * 100
        print('Processing ', (i+1), 'of ', (len(images)/2), 'paired items')
        print("Progress:", progress, '%')

        # Step 5: Get the S3 URLs
        sc1_url = f"https://{bucket_name}.s3.amazonaws.com/{sc1_key}"
        sc2_url = f"https://{bucket_name}.s3.amazonaws.com/{sc2_key}"

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Step 6: Download images to the temporary directory
            sc1_local_path = os.path.join(temp_dir, os.path.basename(sc1_key))
            sc2_local_path = os.path.join(temp_dir, os.path.basename(sc2_key))

            # Download sc1
            s3.download_file(bucket_name, sc1_key, sc1_local_path)

            # Download sc2
            s3.download_file(bucket_name, sc2_key, sc2_local_path)

            # Step 7: Call OCR API for sc1 (Article Screenshot)
            with open(sc1_local_path, 'rb') as sc1_file:
                files = {'file': sc1_file}
                data_article = {'prompt': article_prompt}
                 
                try:
                    response_article = requests.post(OCR_API_URL, files=files, data=data_article)
                    response_article.raise_for_status()
                    
                    # Clean up the response text
                    response_text = response_article.text.strip()
                    
                    # Remove code block markers if present
                    if response_text.startswith('```') and response_text.endswith('```'):
                        response_text = response_text.strip('```').strip()
                        if response_text.startswith('json'):
                            response_text = response_text[len('json'):].strip()
                    
                    # Parse the cleaned JSON
                    result_article = json.loads(response_text)
                    
                except Exception as e:
                    message = f"Error processing article image {sc1_key}: {e}"
                    print(message)
                    logging.error(message)
                    continue
                  
            # Step 8: Call OCR API for sc2 (Reactions Screenshot)
            with open(sc2_local_path, 'rb') as sc2_file:
                files = {'file': sc2_file}
                data_reactions = {'prompt': reactions_prompt}
                try:
                    response_reactions = requests.post(OCR_API_URL, files=files, data=data_reactions)
                    response_reactions.raise_for_status()
                    
                    # Clean up the response text
                    response_text = response_reactions.text.strip()
                    
                    # Remove code block markers if present
                    if response_text.startswith('```') and response_text.endswith('```'):
                        response_text = response_text.strip('```').strip()
                        if response_text.startswith('json'):
                            response_text = response_text[len('json'):].strip()
                    
                    # Parse the cleaned JSON
                    result_reactions = json.loads(response_text)
                except Exception as e:
                    message = f"Error processing reactions image {sc2_key}: {e}"
                    print(message)
                    logging.error(message)
                    continue

            # Step 9: Extract timestamps from filenames
            sc1_date = extract_timestamp(sc1_key)
            sc2_date = extract_timestamp(sc2_key)
            print(' ')

            # Step 10: Compile data into a dictionary
            record = {
                'news_org': news_org,
                'title': result_article.get('title', ''),
                'summary': result_article.get('summary', ''),
                'article_screenshot_s3_link': sc1_url,
                'reactions_screenshot_s3_link': sc2_url,
                'article_sc_date': sc1_date.strftime('%Y-%m-%d %H:%M:%S'),
                'reactions_sc_date': sc2_date.strftime('%Y-%m-%d %H:%M:%S'),
                'total_comments': result_article.get('total_comments', ''),
                'total_shares': result_article.get('total_shares', ''),
                'total_reactions': result_article.get('total_reactions', ''),
                'total_like': result_reactions.get('like', ''),
                'total_love': result_reactions.get('love', ''),
                'total_haha': result_reactions.get('haha', ''),
                'total_wow': result_reactions.get('wow', ''),
                'total_sad': result_reactions.get('sad', ''),
                'total_angry': result_reactions.get('angry', ''),
                'total_care': result_reactions.get('care', ''),
                'unrecognized_total': result_article.get('unrecognized', ''),
                'prompt_used_for_article_scrapping': article_prompt,
                'prompt_used_for_reactions_scrapping': reactions_prompt
            }

            # Step 11: Append the record to the data list
            data.append(record)

        # The temporary directory and its contents are automatically deleted here

    # Step 12: Create a DataFrame and save to CSV
    df = pd.DataFrame(data, columns=headers)
    output_filename = f"{news_org}_output.csv"
    df.to_csv(output_filename, index=False)
    print("Data saved to", output_filename)

    # Calculate elapsed time for the folder processing
    elapsed_time = time.time() - start_time
    message = f"Finished processing folder '{news_org}' in {elapsed_time:.2f} seconds."
    print(message)
    logging.info(message)