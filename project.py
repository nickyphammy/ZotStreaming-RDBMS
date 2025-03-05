import sys

def import_data():
    print("running import_data")

def insertViewer():
    print("running insertViewer")

def addGenre():
    print("running addGenre")

def deleteViewer():
    print("running deleteViewer")

def insertMovie():
    print("running insertMovie")

def insertSession():
    print("running insertSession")

def updateRelease():
    print("running updateRelease")

def listReleases():
    print("running listReleases")

def popularRelease():
    print("running popularRelease")

def releaseTitle():
    print("running releaseTitle")

def activeViewer():
    print("running activeViewer")

def videosViewed():
    print("running videosViewed")


if __name__ == "__main__":

    func_name = sys.argv[1]
    func_args = sys.argv[2:]

    if func_name == "import_data":
        import_data()
    elif func_name == "insertViewer":
        insertViewer()
    elif func_name == "addGenre":
        addGenre()
    elif func_name == "deleteViewer":
        deleteViewer()
    elif func_name == "insertMovie":
        insertMovie()
    elif func_name == "insertSession":
        insertSession()
    elif func_name == "updateRelease":
        updateRelease()
    elif func_name == "listReleases":
        listReleases()
    elif func_name == "popularRelease":
        popularRelease()
    elif func_name == "releaseTitle":
        releaseTitle()
    elif func_name == "activeViewer":
        activeViewer()
    elif func_name == "videosViewed":
        videosViewed()
    else:
        print("Invalid function name selected!")


