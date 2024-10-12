## This script is used to insert data into the database
import os
import json

import psycopg2
from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd
import numpy as np
from glob import glob


from utils import fast_pg_insert

# collect embedding and document file paths
embedding_file_paths = glob(
    "/Users/spencer/PycharmProjects/byu-cs452-labs/recommender/data/embedding/*"
)
embedding_docs_tuples = []

for file_path in embedding_file_paths:
    embedding_base_name = os.path.basename(file_path)
    doc_paths = glob(
        f"/Users/spencer/PycharmProjects/byu-cs452-labs/recommender/data/documents/*{embedding_base_name}"
    )
    embedding_docs_tuples.append((file_path, doc_paths[0]))

# initialize segment and podcast dataframes
segment_df = pd.DataFrame(
    {'id': pd.Series(dtype='str'),
    'start_time': pd.Series(dtype='float'),
    'end_time': pd.Series(dtype='float'),
    'content': pd.Series(dtype='str'),
    # 'embedding': np.ndarray,
    'podcast_id': pd.Series(dtype='str')},
)

podcast_df = pd.DataFrame(
    {'id': pd.Series(dtype='str'),
    'title': pd.Series(dtype='str')}
)

podcast_ids = {}  # set to keep track of podcast ids

for embedding_file, doc_file in embedding_docs_tuples:
    # load data
    with open(embedding_file) as f:
        embeddings = {json.loads(line)['custom_id']: json.loads(line) for line in f}

    with open(doc_file) as f:
        documents = {json.loads(line)['custom_id']: json.loads(line) for line in f}

    # iterate over documents
    segment_data: list[dict] = []
    podcast_data: list[dict] = []
    for segment_id in documents.keys():
        # extract data
        segment_content = documents[segment_id]['body']['input']
        podcast_title = documents[segment_id]['body']['metadata']['title']
        segment_start_time = documents[segment_id]['body']['metadata']['start_time']
        segment_end_time = documents[segment_id]['body']['metadata']['stop_time']
        podcast_id = documents[segment_id]['body']['metadata']['podcast_id']
        segment_embedding = embeddings[segment_id]['response']['body']['data'][0]['embedding']

        # save data representing a row in the segment table
        segment_data.append({
            'id': segment_id,
            'start_time': segment_start_time,
            'end_time': segment_end_time,
            'content': segment_content,
            'podcast_id': podcast_id,
            # 'embedding': segment_embedding
        })

        # save data representing a row in the podcast table if it is not already present
        if podcast_id not in podcast_ids:
            podcast_ids[podcast_id] = podcast_title
            podcast_data.append({
                'id': podcast_id,
                'title': podcast_title
            })

    # load data into dataframes
    segment_df = pd.concat([segment_df, pd.DataFrame(segment_data)])
    podcast_df = pd.concat([podcast_df, pd.DataFrame(podcast_data)])

# set index

# prep to insert into postgres
load_dotenv()  # need to run this to connect to the database
connection = os.getenv("CONNECTION_STRING")

# insert data into postgres
fast_pg_insert(
    df=podcast_df,
    connection=connection,
    table_name='podcast',
    columns=list(podcast_df.columns)
)

fast_pg_insert(
    df=segment_df,
    connection=connection,
    table_name='podcast_segment',
    columns=list(segment_df.columns)
)


# TODO: Read documents files

# HINT: In addition to the embedding and document files you likely need to load the raw data via the hugging face datasets library


# TODO: Insert into postgres
# HINT: use the recommender.utils.fast_pg_insert function to insert data into the database
# otherwise inserting the 800k documents will take a very, very long time