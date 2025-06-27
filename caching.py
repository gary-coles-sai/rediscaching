import psycopg2
import redis
import json
import os
import argparse

# --- Configuration ---
# Replace with your actual AWS RDS PostgreSQL credentials
PG_HOST = os.getenv('PG_HOST', 'your-aws-rds-postgresql-endpoint')
PG_DBNAME = os.getenv('PG_DBNAME', 'your_database_name')
PG_USER = os.getenv('PG_USER', 'your_username')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'your_password')

# Replace with your actual Redis endpoint and port
REDIS_HOST = os.getenv('REDIS_HOST', 'your-aws-elasticache-redis-endpoint')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

CACHE_EXPIRY_SECONDS = 120 # Data in cache expires after 120 seconds (2 minutes)

# --- Database Connection ---
def get_pg_connection():
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            database=PG_DBNAME,
            user=PG_USER,
            password=PG_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

# --- Redis Connection ---
def get_redis_connection():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        r.ping() # Test connection
        return r
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        return None

# --- Main Logic ---
def fetch_data_with_cache(query_key, sql_query):
    r = get_redis_connection()
    pg_conn = get_pg_connection()

    if not r or not pg_conn:
        print("Failed to establish connections to Redis or PostgreSQL.")
        return None

    try:
        # 1. Try to get data from Redis cache
        cached_data = r.get(query_key)
        if cached_data:
            print("Loaded data from redis cache.")
            return json.loads(cached_data.decode('utf-8'))
        
        # 2. If not in cache, fetch from PostgreSQL
        print("Cache miss. Fetching data from PostgreSQL.")
        cur = pg_conn.cursor()
        cur.execute(sql_query)
        db_data = cur.fetchall()
        cur.close()

        # Convert fetched data to a JSON-serializable format
        # Example: if db_data is a list of tuples, convert to list of lists/dicts if needed
        # For simplicity, we'll assume it's directly serializable or convertable to string
        
        # 3. Store data in Redis cache with expiry
        r.setex(query_key, CACHE_EXPIRY_SECONDS, json.dumps(db_data))
        print("Updated redis with PSQL data.")
        return db_data

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        if pg_conn:
            pg_conn.close()

if __name__ == "__main__":
    # Setup argument parser for command line inputs
    parser = argparse.ArgumentParser(description="Fetch data using Redis cache with PostgreSQL.")
    parser.add_argument('-k', '--key', required=True, help='The Redis key to store/retrieve data.')
    parser.add_argument('-q', '--query', required=True, help='The SQL query to execute if data is not in cache.')
    args = parser.parse_args()

    # Example Usage:
    # python your_script_name.py -k 'my_book_data' -q 'SELECT id, title, author FROM books LIMIT 5;'
    # python your_script_name.py -k 'recent_movies' -q 'SELECT title, release_year FROM movies ORDER BY release_year DESC LIMIT 3;'
    
    data = fetch_data_with_cache(args.key, args.query)
    if data is not None:
        print("Retrieved Data:")
        print(data)

