import psycopg2
import database_manager as dbmgr
import api_manager as apimgr

db_manager = dbmgr.DatabaseManager()
api_manager = apimgr.APIManager()

def initialise_dbs():
    db_manager.initialise_dbs()

if __name__ == "__main__":
    initialise_dbs()