import pandas as pd
from google.cloud import storage
import datetime
import uuid
from io import BytesIO
import functions_framework
import requests

# storage client
storage_client = storage.Client()

# storage bucket
bucket_name = "ba882-team05"

# file URLs
netflix_countries_url = "https://www.netflix.com/tudum/top10/data/all-weeks-countries.tsv"
netflix_global_url = "https://www.netflix.com/tudum/top10/data/all-weeks-global.tsv"
netflix_most_popular_url = "https://www.netflix.com/tudum/top10/data/most-popular.tsv"

def process_dataset(url, dataset_name, job_id):
    response = requests.get(url)
    if response.status_code == 200:
        blob_name = f"jobs/{dataset_name}/{job_id}/{dataset_name}.json"
        df = pd.read_csv(BytesIO(response.content), sep='\t')
        
        if dataset_name == "netflix_most_popular":
            # Add the date column for the Tuesday of the current week
            tuesday_date = (datetime.datetime.now() - datetime.timedelta(days=datetime.datetime.now().weekday()) + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            df['extraction_date'] = tuesday_date
        
        json_data = df.to_json(orient='records', lines=True)
        
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(json_data, content_type='application/json')
        
        return {'status': 'success', 'blob_name': blob_name}
    else:
        return {'status': 'error', 'message': f'Failed to fetch data for {dataset_name}', 'status_code': response.status_code}

@functions_framework.http
def main(request):
    JOB_ID = datetime.datetime.now().strftime("%Y%m%d%H%M%S") + "-" + str(uuid.uuid4())
    
    results = {}
    
    # Process Netflix Countries dataset
    results['netflix_countries'] = process_dataset(netflix_countries_url, 'netflix_countries', JOB_ID)
    
    # Process Netflix Global dataset
    results['netflix_global'] = process_dataset(netflix_global_url, 'netflix_global', JOB_ID)
    
    # Process Netflix Most Popular dataset
    results['netflix_most_popular'] = process_dataset(netflix_most_popular_url, 'netflix_most_popular', JOB_ID)
    
    # Check if all datasets were processed successfully
    if all(result['status'] == 'success' for result in results.values()):
        return {
            'message': 'All datasets processed successfully',
            'job_id': JOB_ID,
            'results': results
        }, 200
    else:
        return {
            'message': 'Some datasets failed to process',
            'job_id': JOB_ID,
            'results': results
        }, 500