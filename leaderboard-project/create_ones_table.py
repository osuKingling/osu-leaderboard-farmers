import database_manager as dbmgr

db_manager = dbmgr.DatabaseManager()


def create_ones_table():
    db_manager.create_ones_table()


if __name__ == "__main__":
    create_ones_table()