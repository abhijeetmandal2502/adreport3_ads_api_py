import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# Get database connection parameters from .env file
DB_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    """Create a database connection."""
    try:
        # Explicitly use connection parameters instead of connection string
        # to ensure proper handling of remote connections
        conn = psycopg2.connect(DB_URL, 
                               sslmode='prefer')
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"Connection error: {str(e)}")
        raise

def execute_query(query):
    """Execute a raw SQL query and return the results."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query)
            if cursor.description:  # Check if query returns data
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                # Convert to list of dictionaries
                return [dict(zip(columns, row)) for row in rows]
            return []
    finally:
        conn.close() 