import sys
from data_import import import_data
from user_operations import insertViewer, addGenre, deleteViewer, activeViewer
from release_operations import insertMovie, updateRelease, listReleases, popularRelease, releaseTitle
from session_operations import insertSession, videosViewed

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Fail")
        sys.exit(1)

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

   # for submission: zip submission.zip project.py db_connection.py data_import.py user_operations.py release_operations.py session_operations.py requirements.txt

