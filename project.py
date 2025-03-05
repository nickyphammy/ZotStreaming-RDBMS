import sys

def import_data(args):
    print("running import_data")

def insertViewer(args):
    print("running insertViewer")

def addGenre(args):
    print("running addGenre")

def deleteViewer(args):
    print("running deleteViewer")

def insertMovie(args):
    print("running insertMovie")

def insertSession(args):
    print("running insertSession")

def updateRelease(args):
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

    if func_name == "import_data":
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


