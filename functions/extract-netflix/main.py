import requests
import pandas as pd
import time
from datetime import datetime
import uuid
from google.cloud import storage
import json
from io import BytesIO
import functions_framework

# Storage client
storage_client = storage.Client()

# Storage bucket
bucket_name = "ba882-team05"

# API endpoints and headers
url = "https://streaming-availability.p.rapidapi.com/shows/search/filters"

headers = {
    2024: {
        "x-rapidapi-key": "d09c1a6025msh2fc9d9fc6859b94p1cb6c0jsne5d0e7ed8f7c",
        "x-rapidapi-host": "streaming-availability.p.rapidapi.com"
    },
    2023: {
        "x-rapidapi-key": "23d33399e2mshc636b841c2b78c5p1c766ajsn4a1fd85dbc6f",
        "x-rapidapi-host": "streaming-availability.p.rapidapi.com"
    },
    2022: {
        "x-rapidapi-key": "2a52ed0c7amsh1a7c72c3cc2ab02p1b5d85jsn70ec303c60d0",
        "x-rapidapi-host": "streaming-availability.p.rapidapi.com"
    },
    2021: {
        "x-rapidapi-key": "d3520e3b88mshef627c338af7154p1a3bcejsna458ff60d6cd",
        "x-rapidapi-host": "streaming-availability.p.rapidapi.com"
    },
    2020: {
        "x-rapidapi-key": "36730f9569msh76d57ce02bee8dcp1245c3jsn4359bc78da13",
        "x-rapidapi-host": "streaming-availability.p.rapidapi.com"
    }
}

# columns_to_exclude = [
#     'imageSet.verticalPoster.w240', 'imageSet.verticalPoster.w360',
#     'imageSet.verticalPoster.w480', 'imageSet.verticalPoster.w600',
#     'imageSet.verticalPoster.w720', 'imageSet.horizontalPoster.w360',
#     'imageSet.horizontalPoster.w480', 'imageSet.horizontalPoster.w720',
#     'imageSet.horizontalPoster.w1080', 'imageSet.horizontalPoster.w1440',
#     'imageSet.horizontalBackdrop.w360', 'imageSet.horizontalBackdrop.w480',
#     'imageSet.horizontalBackdrop.w720', 'imageSet.horizontalBackdrop.w1080',
#     'imageSet.horizontalBackdrop.w1440', 'streamingOptions.us'
# ]

columns_to_include = [
    'itemType', 'showType',
    'id', 'imdbId',
    'tmdbId', 'title',
    'overview', 'releaseYear',
    'originalTitle', 'genres',
    'directors', 'cast',
    'rating', 'runtime',
    'firstAirYear', 'lastAirYear',
    'creators', 'seasonCount', 'episodeCount'
]

def fetch_data(year, headers):
    querystring = {
        "series_granularity": "show",
        "order_direction": "asc",
        "order_by": "original_title",
        "genres_relation": "and",
        "output_language": "en",
        "country": "us",
        "catalogs": 'Netflix',
        "year_min": year,
        "year_max": year,
        "cursor": ""
    }
    
    all_results = []
    page_count = 0
    
    while True:
        response = requests.get(url, headers=headers, params=querystring)
        data = response.json()
        
        if 'message' in data:
            print(f"Error fetching data: {data['message']}")
            break
        
        shows = data.get('shows', [])
        
        # Filter out the excluded columns
        filtered_shows = []
        for show in shows:
            filtered_show = {k: v for k, v in show.items() if k in columns_to_include}
            filtered_shows.append(filtered_show)
        
        all_results.extend(filtered_shows)
        page_count += 1
        print(f"Fetched page {page_count}. Total shows: {len(all_results)}")
        
        next_cursor = data.get('nextCursor')
        if not next_cursor:
            break
        
        querystring['cursor'] = next_cursor
        time.sleep(1)  # Add a small delay between requests
    
    return all_results

@functions_framework.http
def main(request):
    
    # ### Only for first load (one-time load)
    # all_data = []
    # for year in [2020, 2021, 2022, 2023]:
    #     print(f"Fetching data for year {year}")
    #     year_data = fetch_data(year, headers[year])
    #     for show in year_data:
    #         show['year'] = year  # Add year information to each show
    #     all_data.extend(year_data)
    #     print(f"Total shows for {year}: {len(year_data)}")
    #     time.sleep(2)  # Pause between years to avoid rate limits

    ### For daily loads - getting latest data
    current_year = datetime.now().year
    print(f"Fetching data for the current year")
    all_data = fetch_data(current_year, headers[current_year])
    for show in all_data:
        show['year'] = current_year  # Add year information to each show 
    print(f"Total shows for {current_year}: {len(all_data)}")

    # Convert to DataFrame
    df = pd.json_normalize(all_data)
    
    # Generate job ID
    JOB_ID = datetime.now().strftime("%Y%m%d%H%M") + "-" + str(uuid.uuid4())

    # Prepare data for GCS
    blob_name = f"jobs/netflix_api/{JOB_ID}/netflix_api.json"

    # Save the data as a json file in memory
    json_buffer = BytesIO()
    for record in all_data:
        json_buffer.write((json.dumps(record) + "\n").encode('utf-8'))
    json_buffer.seek(0)

    # Upload the JSON file to GCS
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_file(json_buffer, content_type="application/json")

    # Prepare results
    file_path = f"gs://{bucket_name}/{blob_name}"
    results = {
        'filepath': file_path,
        'jobid': JOB_ID,
        'bucket_id': bucket_name,
        'blob_name': blob_name,
        'total_shows': len(all_data)
    }
    
    print(results)
    return results