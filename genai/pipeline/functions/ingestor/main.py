# imports
import functions_framework
from google.cloud import secretmanager
import duckdb
import pandas as pd
from pinecone import Pinecone, ServerlessSpec
import json

import vertexai
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel

from langchain.text_splitter import RecursiveCharacterTextSplitter

# settings
project_id = 'ba882-sekka'
region_id = 'us-central1'
secret_id = 'duckdb-token'
version_id = 'latest'
vector_secret = "pinecone"

# db setup
db = 'entertainment'
movie_schema = "movie_embeddings"
show_schema = "show_embeddings"
movie_db_schema = f"{db}.{movie_schema}"
show_db_schema = f"{db}.{show_schema}"
vector_index = "overview-content"

vertexai.init(project=project_id, location=region_id)

@functions_framework.http
def task(request):
    # Parse the request data
    request_json = request.get_json(silent=True)
    print(f"request: {json.dumps(request_json)}")

    # read in from request
    content_id = request_json.get('content_id')
    content_type = request_json.get('content_type')  # 'movie' or 'show'

    # instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # initiate the MotherDuck connection through an access token
    md = duckdb.connect(f'md:?motherduck_token={md_token}')

    # get the content
    if content_type == 'movie':
        content_df = md.sql(f"SELECT * FROM {db}.stage.netflix_api WHERE id = '{content_id}' AND showType = 'movie' LIMIT 1;").df()
        db_schema = movie_db_schema
        table_name = 'pinecone_movies'
    else:
        content_df = md.sql(f"SELECT * FROM {db}.stage.netflix_api WHERE id = '{content_id}' AND showType = 'series' LIMIT 1;").df()
        db_schema = show_db_schema
        table_name = 'pinecone_shows'

    # content_df['timestamp'] = pd.to_datetime(content_df['release_year'], format='%Y').astype('int64') // 10**9

    # connect to pinecone
    vector_name = f"projects/{project_id}/secrets/{vector_secret}/versions/{version_id}"
    response = sm.access_secret_version(request={"name": vector_name})
    pinecone_token = response.payload.data.decode("UTF-8")
    pc = Pinecone(api_key=pinecone_token)
    index = pc.Index(vector_index)
    print(f"index stats: {index.describe_index_stats()}")

    # setup the embedding model
    MODEL_NAME = "text-embedding-005"
    DIMENSIONALITY = 768
    task = "RETRIEVAL_DOCUMENT"
    model = TextEmbeddingModel.from_pretrained(MODEL_NAME)

    # setup the splitter
    chunk_docs = []
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=350,
        chunk_overlap=75,
        length_function=len,
        is_separator_regex=False,
    )

    # iterate over the row
    content_data = content_df.to_dict(orient="records")[0]
    text = content_data['overview'].replace('\xa0', ' ')
    chunks = text_splitter.create_documents([text])
    id = content_data['id']
    for cid, chunk in enumerate(chunks):
        chunk_text = chunk.page_content
        input = TextEmbeddingInput(chunk_text, task)
        embedding = model.get_embeddings([input])
        chunk_doc = {
            'id': id + '_' + str(cid),
            'values': embedding[0].values,
            'metadata': {
                'title': content_data['title'],
                'chunk_index': cid,
                'content_id': id,
                'chunk_text': chunk_text,
                'content_type': content_type
            }
        }
        chunk_docs.append(chunk_doc)
    
    # flatten to dataframe for the ingestion
    chunk_df = pd.DataFrame(chunk_docs)
    print(f"{content_type} id {id} has {len(chunk_df)} chunks")

    # upsert to pinecone
    index.upsert_from_dataframe(chunk_df, batch_size=100)

    # add record to warehouse 
    md.sql(f"INSERT INTO {db_schema}.{table_name} (id, parsed_timestamp) VALUES ('{id}');")
    print(f"{content_type} {id} added to the warehouse for job tracking")

    # finish the job
    return {}, 200