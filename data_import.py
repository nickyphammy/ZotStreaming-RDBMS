import os
import csv
import mysql.connector
from db_connection import get_db_connection

def import_data(args) -> bool:
    if len(args) != 1:
        print("Fail")
        return False
    
    folder_name = args[0]
    
    # Check if folder exists
    if not os.path.exists(folder_name):
        print("Fail")
        return False
    
    # Make sure all required CSV files exist
    tables = ["users", "producers", "viewers", "releases", "movies", "series", "videos", "sessions", "reviews"]
    for table in tables:
        file_path = os.path.join(folder_name, f"{table}.csv")
        if not os.path.exists(file_path):
            print("Fail")
            return False
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # Drop existing tables if they exist (in reverse order of creation to handle foreign key constraints)
        drop_tables = ["reviews", "sessions", "videos", "series", "movies", "releases", "viewers", "producers", "users"]
        for table in drop_tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        
        # Create tables based on the ER diagram
        ddl_statements = [
            """
            CREATE TABLE users (
                uid INT,
                email TEXT NOT NULL,
                joined_date DATE NOT NULL,
                nickname TEXT NOT NULL,
                street TEXT,
                city TEXT,
                state TEXT,
                zip TEXT,
                genres TEXT,
                PRIMARY KEY (uid)
            )
            """,
            """
            CREATE TABLE producers (
                uid INT,
                bio TEXT,
                company TEXT,
                PRIMARY KEY (uid),
                FOREIGN KEY (uid) REFERENCES users(uid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE viewers (
                uid INT,
                subscription ENUM('free', 'monthly', 'yearly'),
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                PRIMARY KEY (uid),
                FOREIGN KEY (uid) REFERENCES users(uid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE releases (
                rid INT,
                producer_uid INT NOT NULL,
                title TEXT NOT NULL,
                genre TEXT NOT NULL,
                release_date DATE NOT NULL,
                PRIMARY KEY (rid),
                FOREIGN KEY (producer_uid) REFERENCES producers(uid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE movies (
                rid INT,
                website_url TEXT,
                PRIMARY KEY (rid),
                FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE series (
                rid INT,
                introduction TEXT,
                PRIMARY KEY (rid),
                FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE videos (
                rid INT,
                ep_num INT NOT NULL,
                title TEXT NOT NULL,
                length INT NOT NULL,
                PRIMARY KEY (rid, ep_num),
                FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE sessions (
                sid INT,
                uid INT NOT NULL,
                rid INT NOT NULL,
                ep_num INT NOT NULL,
                initiate_at DATETIME NOT NULL,
                leave_at DATETIME NOT NULL,
                quality ENUM('480p', '720p', '1080p'),
                device ENUM('mobile', 'desktop'),
                PRIMARY KEY (sid),
                FOREIGN KEY (uid) REFERENCES viewers(uid) ON DELETE CASCADE,
                FOREIGN KEY (rid, ep_num) REFERENCES videos(rid, ep_num) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE reviews (
                rvid INT,
                uid INT NOT NULL,
                rid INT NOT NULL,
                rating DECIMAL(2, 1) NOT NULL CHECK (rating BETWEEN 0 AND 5),
                body TEXT,
                posted_at DATETIME NOT NULL,
                PRIMARY KEY (rvid),
                FOREIGN KEY (uid) REFERENCES viewers(uid) ON DELETE CASCADE,
                FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE
            )
            """
        ]
        
        for statement in ddl_statements:
            cursor.execute(statement)
        
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

        # Load data from CSV files
        for table in tables:
            file_path = os.path.join(folder_name, f"{table}.csv")
            
            # Read the CSV and insert data
            with open(file_path, 'r') as file:
                csv_reader = csv.reader(file)
                # Skip header row if it exists
                header = next(csv_reader, None)
                
                for row in csv_reader:
                    # Clean any empty strings
                    row = [None if item == '' else item for item in row]
                    
                    # Create placeholders for the SQL query
                    placeholders = ', '.join(['%s'] * len(row))
                    query = f"INSERT INTO {table} VALUES ({placeholders})"
                    
                    cursor.execute(query, row)
        
        conn.commit()
        print("Success")
        return True
    except Exception as e:
        conn.rollback()
        print("Fail")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close() 