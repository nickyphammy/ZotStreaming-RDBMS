import mysql.connector
from db_connection import get_db_connection

def insertMovie(args) -> bool:
    if (len(args) < 2):
        print("Fail")
        return False
    rid = int(args[0])
    website_url = args[1]

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Check if the release exists
        check_release_query = "SELECT rid FROM releases WHERE rid = %s"
        cursor.execute(check_release_query, (rid,))
        if not cursor.fetchone():
            # Release doesn't exist
            print("Fail")
            return False
        
        # Insert into movies table
        insert_movie_query = """
        INSERT INTO movies (rid, website_url)
        VALUES (%s, %s)
        """
        movie_values = (rid, website_url)
        cursor.execute(insert_movie_query, movie_values)
        
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


def updateRelease(args) -> bool:
    if (len(args) != 2):
        print("Fail")
        return False
    
    rid = args[0]
    title = args[1]

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Check if the release exists
        check_release_query = "SELECT rid FROM releases WHERE rid = %s"
        cursor.execute(check_release_query, (rid,))
        if not cursor.fetchone():
            print("Fail")
            return False
        
        # Update release table
        update_release_query = """
        UPDATE releases SET title = %s WHERE rid = %s
        """
        release_values = (title, rid)
        cursor.execute(update_release_query, release_values)
        
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


def listReleases(args):
    if len(args) < 1:
        print("Fail")
        return False
    
    uid = args[0]

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Query releases table
        list_viewer_query = """
            SELECT DISTINCT r.rid, r.genre, r.title
            FROM reviews rv
            JOIN releases r ON rv.rid = r.rid
            WHERE rv.uid = %s
            ORDER BY r.title ASC;       
            """
        viewer_values = (uid, )
        cursor.execute(list_viewer_query, viewer_values)
        list_releases = cursor.fetchall()

        for row in list_releases:
            print(f"{row[0]},{row[1]},{row[2]}")
        
        return True

    except Exception as err:
        # Rollback in case of error
        conn.rollback()
        print("Fail")
        return False
    
    finally:
        cursor.close()
        conn.close()


def popularRelease(args):
    if len(args) != 1:
        print("Fail")
        return False
    try:
        N = int(args[0])
        if N < 0:
            print("Fail")
            return False
    except ValueError:
        print("Fail")
        return False
    
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = """
            SELECT r.rid, r.title, COUNT(DISTINCT rv.rvid) AS reviewCount
            FROM releases r
            LEFT JOIN reviews rv ON r.rid = rv.rid
            GROUP BY r.rid, r.title
            ORDER BY reviewCount DESC, r.rid DESC
            LIMIT %s
        """

        cursor.execute(query, (N,))
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


def releaseTitle(args):

    print("running releaseTitle")
    return False 