import mysql.connector
import sys

def get_db_connection():
    """Create and return a database connection"""
    try:
        # Connect to the MySQL database
        conn = mysql.connector.connect(
            user='test',
            password='password',
            database='cs122a'
        )
        return conn
    except mysql.connector.Error as err:
        print("Fail")
        sys.exit(1) 