## This script is used to create the tables in the database

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

CONNECTION = os.getenv("CONNECTION_STRING")

# need to run this to enable vector data type
CREATE_EXTENSION = "CREATE EXTENSION vector"

# TODO: Add create table statement
CREATE_PODCAST_TABLE = """
CREATE TABLE podcast(
    id TEXT PRIMARY KEY,
    title TEXT
);
"""
# TODO: Add create table statement
CREATE_SEGMENT_TABLE = """
CREATE TABLE podcast_segment(
    id TEXT PRIMARY KEY,
    start_time FLOAT,
    end_time FLOAT,
    content TEXT,
    embedding VECTOR(128),
    podcast_id  TEXT REFERENCES podcast(id)
);
"""

# COMPLETED: Create tables with psycopg2 (example: https://www.geeksforgeeks.org/executing-sql-query-with-psycopg2-in-python/)
conn = psycopg2.connect(CONNECTION)
conn.autocommit = True

cursor = conn.cursor()

# cursor.execute(CREATE_EXTENSION)
cursor.execute(CREATE_PODCAST_TABLE)
cursor.execute(CREATE_SEGMENT_TABLE)

cursor.close()
conn.close()
