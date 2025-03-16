import mysql.connector
from datetime import datetime
from db_connection import get_db_connection

def insertViewer(args) -> bool:
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
        # Check if user already exists
        check_user_query = "SELECT uid FROM users WHERE uid = %s"
        cursor.execute(check_user_query, (uid,))
        if cursor.fetchone():
            # User already exists
            print("Fail")
            return False

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
    if len(args) < 2:
        print("Fail")
        return False
    
    # Parse arguments
    uid = int(args[0])
    new_genre = args[1]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # First, get the current genres for the user
        get_genres_query = "SELECT genres FROM users WHERE uid = %s"
        cursor.execute(get_genres_query, (uid,))
        result = cursor.fetchone()
        
        if not result:
            # User not found
            print("Fail")
            return False
            
        current_genres = result[0] # Index 0 because fetchone() returns a tuple of values
        
        # Update the genres list
        if current_genres is None or current_genres == '':
            # If the user has no genres yet
            updated_genres = new_genre
        else:
            # Split the current genres and check if the new genre already exists
            genres_list = current_genres.split(';')
            if new_genre.lower() not in [g.lower() for g in genres_list]:
                # Only add if it doesn't already exist (case-insensitive check)
                updated_genres = current_genres + ';' + new_genre
            else:
                # Genre already exists, no update needed
                updated_genres = current_genres
        
        # Update the user's genres
        update_query = "UPDATE users SET genres = %s WHERE uid = %s"
        cursor.execute(update_query, (updated_genres, uid))
        
        # Check if the update was successful (rows affected)
        if cursor.rowcount > 0:
            conn.commit()
            print("Success")
            return True
        else:
            conn.rollback()
            print("Fail")
            return False
    
    except mysql.connector.Error as err:
        # Rollback in case of error
        conn.rollback()
        print("Fail")
        return False
    
    finally:
        cursor.close()
        conn.close()


def deleteViewer(args) -> bool:
    if len(args) < 1:
        print("Fail")
        return False
    
    # Parse arguments
    uid = int(args[0])
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Start transaction
        conn.start_transaction()
        
        # Delete from viewers table
        delete_viewer_query = "DELETE FROM viewers WHERE uid = %s"
        cursor.execute(delete_viewer_query, (uid,))
        
        # Delete from users table
        delete_user_query = "DELETE FROM users WHERE uid = %s"
        cursor.execute(delete_user_query, (uid,))
        
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


def activeViewer(args):
    if len(args) != 3:
        print("Fail")
        return False
    
    # Initialize cursor and conn to None
    cursor = None
    conn = None
    
    try:
        N = int(args[0])
        start_date = args[1]
        end_date = args[2]
        
        if N < 1:
            print("Fail")
            return False
        
        # Handle date parsing more robustly
        try:
            # Extract just the date part (before any space or time component)
            if ' ' in start_date:
                start_date = start_date.split(' ')[0]
            if ' ' in end_date:
                end_date = end_date.split(' ')[0]
                
            # Parse dates with just the date format
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
            
            if end_date_obj < start_date_obj:
                print("Fail")
                return False
        except ValueError:
            print("Fail")
            return False
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT v.uid, v.first_name, v.last_name
            FROM viewers v
            JOIN sessions s ON v.uid = s.uid
            WHERE DATE(s.initiate_at) >= %s AND DATE(s.initiate_at) <= %s
            GROUP BY v.uid, v.first_name, v.last_name
            HAVING COUNT(s.sid) >= %s
            ORDER BY v.uid ASC
        """
        
        # Correct parameter order to match the query placeholders
        cursor.execute(query, (start_date, end_date, N))
        results = cursor.fetchall()
        
        for row in results:
            print(f"{row[0]},{row[1]},{row[2]}")
        
        return True
        
    except Exception:
        print("Fail")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close() 