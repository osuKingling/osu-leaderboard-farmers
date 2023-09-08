import time
import pytz
import database_manager as dbmgr
import api_manager as apimgr
from datetime import datetime as dt, timedelta
import datetime

db_manager = dbmgr.DatabaseManager()
api_manager = apimgr.APIManager()

def update_all_maps():
    utc = pytz.UTC
    current_datetime = datetime.datetime(2007, 9, 1, 0, 0, 0)
    current_date_time = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    completed = False

    while not completed:
        time.sleep(1 / 2)
        maps_input = api_manager.retrieve_beatmaps(since=current_date_time)
        print(current_date_time)
        if len(maps_input) < 500:
            completed = True

        for beatmap in maps_input:
            old_date = dt.strptime(current_date_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc)
            new_date = beatmap.approved_date.replace(tzinfo=utc)
            if new_date > old_date:
                current_date_time = (new_date + timedelta(seconds=-1)).strftime("%Y-%m-%d %H:%M:%S")

            if beatmap.approved in ["4", "2", "1"]:
                db_manager.import_map(beatmap)

if __name__ == '__main__':
    update_all_maps()