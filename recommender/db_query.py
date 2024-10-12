## This script is used to query the database
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

CONNECTION = os.getenv("CONNECTION_STRING") # paste connection string here or read from .env file



# query #1

def query_podcast_segments(podcast_id, get_dissimilar=False):
    order_str = "ASC" if get_dissimilar else "DESC"
    return f"""
SELECT (
    SELECT title FROM podcast WHERE id = podcast_id
), id, content, start_time, end_time, 
l2_distance(embedding, (
        SELECT embedding from podcast_segment WHERE id= '{podcast_id}'
    )
 ) as distance
FROM podcast_segment
WHERE id != '{podcast_id}'
ORDER BY distance {order_str}
LIMIT 5;
"""

QUERY_1 = query_podcast_segments('267:476')

QUERY_2 = query_podcast_segments('267:476', get_dissimilar=True)

QUERY_3 = query_podcast_segments('48:511')

QUERY_4 = query_podcast_segments('51:56', get_dissimilar=True)


def query_episodes_similar_to_segment(segment_id):
    return f"""
SELECT (
    SELECT title FROM podcast WHERE id = podcast_id
    ) as podcast_title,
    l2_distance(AVG(embedding), (
            SELECT embedding from podcast_segment WHERE id= '{segment_id}'
        )
    ) AS distance
    FROM podcast_segment
    GROUP BY podcast_id
    ORDER BY distance
    LIMIT 5;
"""

QUERY_5A = query_episodes_similar_to_segment('267:476')
QUERY_5B = query_episodes_similar_to_segment('48:511')
QUERY_5C = query_episodes_similar_to_segment('51:56')

QUERY_6 = """
SELECT (
    SELECT title FROM podcast WHERE id = podcast_id
    ) as podcast_title,
    l2_distance(AVG(embedding), (
            SELECT AVG(embedding) from podcast_segment WHERE podcast_id = ' VeH7qKZr0WI'
        )
    ) AS distance
    FROM podcast_segment
    GROUP BY podcast_id
    ORDER BY distance
    LIMIT 5;
"""

def execute_query(query: str) -> list[str]:
    """
        Executes a query on the database.

        Parameters:
        query (str): The SQL query to be executed

        Returns:
        list[str]: A list of strings containing the results of the query
    """
    output = []
    with psycopg2.connect(CONNECTION) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        for record in records:
            print(record)
            output.append(str(record) + "\n")

    return output


if __name__ == "__main__":
    print('Query 1:', QUERY_1)
    execute_query(QUERY_1)

    print("\nQuery 2:", QUERY_2)
    execute_query(QUERY_2)

    print("\nQuery 3:", QUERY_3)
    execute_query(QUERY_3)

    print("\nQuery 4:", QUERY_4)
    execute_query(QUERY_4)

    print("\nQuery 5A:", QUERY_5A)
    execute_query(QUERY_5A)

    print("\nQuery 5B:", QUERY_5B)
    execute_query(QUERY_5B)

    print("\nQuery 5C:", QUERY_5C)
    execute_query(QUERY_5C)

    print("\nQuery 6:", QUERY_6)
    execute_query(QUERY_6)
