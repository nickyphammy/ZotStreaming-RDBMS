import mysql.connector
from db_connection import get_db_connection

def insertSession(args) -> bool:
    if (len(args) != 8):
        print("Fail")
        return False
    
    # Store all arguments
    sid = args[0]
    uid = args[1]
    rid = args[2]
    ep_num = args[3]
    initiate_at = args[4]
    leave_at = args[5]
    quality = args[6]
    device = args[7]

    # Establish connection and create sql cursor
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Check if the dependencies exist
        check_viewer_query = "SELECT uid FROM viewers WHERE uid = %s"
        cursor.execute(check_viewer_query, (uid,))
        if not cursor.fetchone():
            print("Fail")
            return False
        
        check_release_query = "SELECT rid FROM releases WHERE rid = %s"
        cursor.execute(check_release_query, (rid,))
        if not cursor.fetchone():
            print("Fail")
            return False
        
        check_video_query = "SELECT rid, ep_num FROM videos WHERE rid = %s AND ep_num = %s"
        cursor.execute(check_video_query, (rid, ep_num, ))
        if not cursor.fetchone():
            print("Fail")
            return False
        
        valid_qualities = {'480p', '720p', '1080p'}
        valid_devices = {'mobile', 'desktop'}

        if quality not in valid_qualities:
            print("Fail")
            return False
        if device not in valid_devices:
            print("Fail")
            return False
        
        # Insert into sessions table
        insert_session_query = """
        INSERT INTO sessions (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        session_values = (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device)
        cursor.execute(insert_session_query, session_values)
        
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


def videosViewed(args):
    # Placeholder for videosViewed function
    print("running videosViewed")
    return False 