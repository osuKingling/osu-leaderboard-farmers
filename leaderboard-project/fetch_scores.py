import database_manager as dbmgr
import api_manager as apimgr
from datetime import datetime as dt


db_manager = dbmgr.DatabaseManager()
api_manager = apimgr.APIManager()


def refresh_all_scores(beatmap_ids):
    batch_size = 50
    for i in range(0, len(beatmap_ids), batch_size):
        print(f"{i}/{len(beatmap_ids)} complete", dt.now().strftime("%H:%M:%S:%f"))
        retrieve_scores = api_manager.retrieve_beatmap_scores(beatmap_ids[i:i+batch_size])
        n = 0
        for scores in retrieve_scores:

            beatmap_id = beatmap_ids[i+n]
            db_manager.import_score(scores, beatmap_id)
            n += 1

    db_manager.create_ones_table()


if __name__ == '__main__':
    refresh_all_scores(db_manager.retrieve_beatmap_ids(0))