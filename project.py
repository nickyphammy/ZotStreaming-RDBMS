import sys
import os
import csv
import mysql.connector
from datetime import datetime

# python -m pip install mysql-connector-python

# Database connection function
def get_db_connection():
    try:
        # Connect to the MySQL database
        conn = mysql.connector.connect(
            user='test',
            password='password',
            database='cs122a'
        )
        print("Connected") # Take this out at the end
        return conn
    except mysql.connector.Error as err:
        print("Fail")
        sys.exit(1)

def import_data(args) -> bool:
    if len(args) != 1:
        print("Fail")
        return False
    
    folder_name = args[0]
    
    # # Print debug info
    # print(f"Current directory: {os.getcwd()}")
    # print(f"Looking for folder: {folder_name}")
    # print(f"Folder exists: {os.path.exists(folder_name)}")
    # if os.path.exists(folder_name):
    #     print(f"Files in folder: {os.listdir(folder_name)}")
    
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
        
        # Drop existing tables if they exist (in reverse order of creation to handle foreign key constraints)
        drop_tables = ["reviews", "sessions", "videos", "series", "movies", "releases", "viewers", "producers", "users"]
        for table in drop_tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        
        # Create tables based on the ER diagram
        ddl_statements = [
            """
            CREATE TABLE Users (
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
            CREATE TABLE Producers (
                uid INT,
                bio TEXT,
                company TEXT,
                PRIMARY KEY (uid),
                FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE Viewers (
                uid INT,
                subscription ENUM('free', 'monthly', 'yearly'),
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                PRIMARY KEY (uid),
                FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE Releases (
                rid INT,
                producer_uid INT NOT NULL,
                title TEXT NOT NULL,
                genre TEXT NOT NULL,
                release_date DATE NOT NULL,
                PRIMARY KEY (rid),
                FOREIGN KEY (producer_uid) REFERENCES Producers(uid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE Movies (
                rid INT,
                website_url TEXT,
                PRIMARY KEY (rid),
                FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE Series (
                rid INT,
                introduction TEXT,
                PRIMARY KEY (rid),
                FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE Videos (
                rid INT,
                ep_num INT NOT NULL,
                title TEXT NOT NULL,
                length INT NOT NULL,
                PRIMARY KEY (rid, ep_num),
                FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE Sessions (
                sid INT,
                uid INT NOT NULL,
                rid INT NOT NULL,
                ep_num INT NOT NULL,
                initiate_at DATETIME NOT NULL,
                leave_at DATETIME NOT NULL,
                quality ENUM('480p', '720p', '1080p'),
                device ENUM('mobile', 'desktop'),
                PRIMARY KEY (sid),
                FOREIGN KEY (uid) REFERENCES Viewers(uid) ON DELETE CASCADE,
                FOREIGN KEY (rid, ep_num) REFERENCES Videos(rid, ep_num) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE Reviews (
                rvid INT,
                uid INT NOT NULL,
                rid INT NOT NULL,
                rating DECIMAL(2, 1) NOT NULL CHECK (rating BETWEEN 0 AND 5),
                body TEXT,
                posted_at DATETIME NOT NULL,
                PRIMARY KEY (rvid),
                FOREIGN KEY (uid) REFERENCES Viewers(uid) ON DELETE CASCADE,
                FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
            )
            """
        ]
        
        for statement in ddl_statements:
            cursor.execute(statement)
        
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

# Function to insert a new Viewer
def insertViewer(args):
    if len(args) < 12:
        print("Fail")
        return False
    
    # Parse arguments
    uid = int(args[0])
    email = args[1]
    nickname = args[2]
    street = args[3]
    city = args[4]
    state = args[5]
    zip_code = args[6]
    genres = args[7]
    joined_date = args[8]
    first_name = args[9]
    last_name = args[10]
    subscription = args[11]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Insert into Users table first
        insert_user_query = """
        INSERT INTO users (uid, email, joined_date, nickname, street, city, state, zip, genres)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        user_values = (uid, email, joined_date, nickname, street, city, state, zip_code, genres)
        cursor.execute(insert_user_query, user_values)
        
        # Insert into Viewers table
        insert_viewer_query = """
        INSERT INTO viewers (uid, subscription, first_name, last_name)
        VALUES (%s, %s, %s, %s)
        """
        viewer_values = (uid, subscription, first_name, last_name)
        cursor.execute(insert_viewer_query, viewer_values)
        
        # Commit the transaction
        conn.commit()
        print("Success")
        return True
    
    except mysql.connector.Error as err:
        # Rollback in case of error
        conn.rollback()
        print("Fail")
        return False
    
    finally:
        cursor.close()
        conn.close()

def addGenre(args) -> bool:
    print("running addGenre")

def deleteViewer(args) -> bool:
    print("running deleteViewer")

def insertMovie(args) -> bool:
    print("running insertMovie")

def insertSession(args) -> bool:
    print("running insertSession")

def updateRelease(args) -> bool:
    print("running updateRelease")

def listReleases(args):
    print("running listReleases")

def popularRelease(args):
    print("running popularRelease")

def releaseTitle(args):
    print("running releaseTitle")

def activeViewer(args):
    print("running activeViewer")

def videosViewed(args):
    print("running videosViewed")


if __name__ == "__main__":

    func_name = sys.argv[1]
    func_args = sys.argv[2:]

    if func_name == "import":
        import_data(func_args)
    elif func_name == "insertViewer":
        insertViewer(func_args)
    elif func_name == "addGenre":
        addGenre(func_args)
    elif func_name == "deleteViewer":
        deleteViewer(func_args)
    elif func_name == "insertMovie":
        insertMovie(func_args)
    elif func_name == "insertSession":
        insertSession(func_args)
    elif func_name == "updateRelease":
        updateRelease(func_args)
    elif func_name == "listReleases":
        listReleases(func_args)
    elif func_name == "popularRelease":
        popularRelease(func_args)
    elif func_name == "releaseTitle":
        releaseTitle(func_args)
    elif func_name == "activeViewer":
        activeViewer(func_args)
    elif func_name == "videosViewed":
        videosViewed(func_args)
    else:
        print("Invalid function name selected!")


