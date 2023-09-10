import os
import ossapi
import psycopg2
from urllib.parse import urlparse


# GET #1s SELECT DISTINCT ON (beatmap_id) score_id FROM scores ORDER BY beatmap_id, score DESC, created_at ASC;
# SELECT scores.*, rank() OVER (PARTITION BY beatmap_id ORDER BY score DESC, created_at ASC) FROM scores;

# WITH ids as (SELECT beatmap_id FROM beatmaps WHERE difficulty_rating < 6)
# SELECT ranked_scores.username, COUNT(ranked_scores.username) FROM
# (SELECT scores.*, rank() OVER (PARTITION BY beatmap_id ORDER BY score DESC, created_at ASC) FROM scores)
# ranked_scores
# WHERE rank = 1 AND ranked_scores.beatmap_id IN (SELECT beatmap_id FROM ids)
# GROUP BY ranked_scores.username ORDER BY COUNT(ranked_scores.username) DESC;

class DatabaseManager:

    def __init__(self):
        db_url = urlparse(os.environ['DATABASE_URL'])
        username = db_url.username
        password = db_url.password
        database = db_url.path[1:]
        hostname = db_url.hostname
        port = db_url.port
        try:
            self.conn = psycopg2.connect(database=database,
                                         user=username,
                                         password=password,
                                         host=hostname,
                                         )
        except Exception:
            print("Unable to connect to DB")

    def initialise_dbs(self):
        queries = ["""
        SET timezone = 'UTC';
        """,
                   """
            CREATE TABLE IF NOT EXISTS beatmaps (
                beatmap_id INT PRIMARY KEY,
                beatmapset_id INT NOT NULL,
                ranked_status INT NOT NULL,
                title TEXT NOT NULL,
                artist TEXT NOT NULL,
                source TEXT NOT NULL,
                diff_name TEXT NOT NULL,
                difficulty_rating float4 NOT NULL,
                bpm float4 NOT NULL,
                ar float4 NOT NULL,
                cs float4 NOT NULL,
                od float4 NOT NULL,
                hp float4 NOT NULL,
                length INT NOT NULL,
                max_combo INT NOT NULL,
                circles INT NOT NULL,
                sliders INT NOT NULL,
                spinners INT NOT NULL,
                submit_date TIMESTAMP NOT NULL,
                approved_date TIMESTAMP NOT NULL,
                playcount INT NOT NULL, 
                creator TEXT NOT NULL,             
                creator_id INT NOT NULL,
                tags TEXT [] NOT NULL,
                mode INT NOT NULL
                
            );           
        """,
        """
            CREATE TABLE IF NOT EXISTS scores (
            score_id BIGINT PRIMARY KEY,
            username TEXT NOT NULL,
            user_id INT NOT NULL,
            beatmap_id INT NOT NULL,
            score BIGINT NOT NULL,
            accuracy float4 NOT NULL,
            combo INT NOT NULL,
            mods INT NOT NULL,
            count_300 INT NOT NULL,
            count_100 INT NOT NULL,
            count_50 INT NOT NULL,
            count_miss INT NOT NULL,
            pp float4 NOT NULL,
            created_at TIMESTAMP NOT NULL,
            rank INT NOT NULL          
           );           
        """,
        """
        CREATE UNIQUE INDEX idx_scores_beatmapid_rank ON scores (beatmap_id, rank);
        """,
        """
            CREATE TABLE IF NOT EXISTS users (
            discord_id BIGINT PRIMARY KEY,
            osu_user_id INT          
           );           
        """]

        try:
            cur = self.conn.cursor()
            for query in queries:
                cur.execute(query)
            cur.close()
            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def import_map(self, beatmap: ossapi.ossapi.Beatmap):
        query = """
            INSERT INTO beatmaps
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """

        try:
            cur = self.conn.cursor()
            cur.execute(query,
                        [beatmap.beatmap_id, beatmap.beatmapset_id, beatmap.approved, beatmap.title, beatmap.artist,
                         beatmap.source, beatmap.version, beatmap.star_rating, beatmap.bpm, beatmap.approach_rate,
                         beatmap.circle_size, beatmap.overrall_difficulty, beatmap.health, beatmap.total_length,
                         beatmap.max_combo, beatmap.count_hitcircles, beatmap.count_sliders, beatmap.count_spinners,
                         beatmap.submit_date, beatmap.approved_date, beatmap.playcount, beatmap.creator,
                         beatmap.creator_id, beatmap.tags.split(), beatmap.mode])
            cur.close()
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def import_score(self, scores, beatmap_id):
        # print("start of db import", dt.now().strftime("%H:%M:%S:%f"))
        query = """
            INSERT INTO scores
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (beatmap_id, rank)
            DO update 
            SET (score_id, username, user_id, score, accuracy, combo, mods, count_300, count_100, count_50, count_miss, pp, created_at) =
            (EXCLUDED.score_id, EXCLUDED.username, EXCLUDED.user_id, EXCLUDED.score, EXCLUDED.accuracy, EXCLUDED.combo, EXCLUDED.mods, EXCLUDED.count_300, EXCLUDED. count_100, EXCLUDED.count_50, EXCLUDED.count_miss, EXCLUDED.pp, EXCLUDED.created_at)
        """

        try:
            cur = self.conn.cursor()
            # cur.execute(f"DELETE FROM scores WHERE user_id = {score['user_id']} AND beatmap_id = {beatmap_id} ")
            rank = 1
            for score in scores:

                if score['pp'] is None:
                    pp = 0
                else:
                    pp = score['pp']

                mod_int = 0

                cur.execute(query,
                            [score['id'], str(score['user']['username']), score['user_id'], beatmap_id, score['score'],
                             score['accuracy'] * 100, score['max_combo'],
                             self.convert_mod_list_to_bitwise(score['mods']),
                             score['statistics']['count_300'], score['statistics']['count_100'],
                             score['statistics']['count_50'],
                             score['statistics']['count_miss'], float(pp), score['created_at'], rank])
                rank += 1

            cur.close()
            self.conn.commit()
            # print("end of db import", dt.now().strftime("%H:%M:%S:%f"))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def retrieve_beatmap_ids(self, mode):
        query = f"""
        SELECT beatmap_id FROM beatmaps WHERE mode = {mode} ORDER BY beatmap_id ASC
        """

        try:
            cur = self.conn.cursor()
            cur.execute(query)
            beatmap_ids = cur.fetchall()
            beatmap_ids = [j[0] for j in beatmap_ids]
            cur.close()
            return beatmap_ids

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def create_ones_table(self):
        query = """CREATE TABLE one_scores AS
                   SELECT * 
                   FROM scores              
                   WHERE rank = 1"""

        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS one_scores")
        cur.execute(query)
        self.conn.commit()
        cur.close()

    def create_temp_scores(self):
        query = """CREATE TABLE IF NOT EXISTS scores_temp (
                    score_id BIGINT PRIMARY KEY,
                    username TEXT NOT NULL,
                    user_id INT NOT NULL,
                    beatmap_id INT NOT NULL,
                    score BIGINT NOT NULL,
                    accuracy float4 NOT NULL,
                    combo INT NOT NULL,
                    mods INT NOT NULL,
                    count_300 INT NOT NULL,
                    count_100 INT NOT NULL,
                    count_50 INT NOT NULL,
                    count_miss INT NOT NULL,
                    pp float4 NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    rank INT NOT NULL          
                    );"""

        cursor = self.conn.cursor()
        cursor.execute(query)
        cursor.close()
        self.conn.commit()

    def delete_temp_scores(self):
        query = "DROP TABLE scores_temp"
        cursor = self.conn.cursor()
        cursor.execute(query)
        cursor.close()
        self.conn.commit()

    def copy_from_temp_to_scores(self):
        queries = ["TRUNCATE scores", "INSERT INTO scores SELECT * FROM scores_temp"]
        cursor = self.conn.cursor()
        for query in queries:
            cursor.execute(query)
        cursor.close()
        self.conn.commit()

    def convert_mod_list_to_bitwise(self, mods):
        mod_int = 0
        for mod in mods:
            if mod == 'NF':
                mod_int += 1
            if mod == 'EZ':
                mod_int += 2
            if mod == 'HD':
                mod_int += 4
            if mod == 'HR':
                mod_int += 8
            if mod == 'SD':
                mod_int += 16
            if mod == 'DT':
                mod_int += 32
            if mod == 'RL':
                mod_int += 64
            if mod == 'HT':
                mod_int += 128
            if mod == 'NC':
                mod_int += 256
            if mod == 'FL':
                mod_int += 512
            if mod == 'SO':
                mod_int += 1024
            if mod == 'PF':
                mod_int += 2048
            if mod == 'TD':
                mod_int += 4096

        return mod_int

    def convert_bitwise_to_mod_list(self, mod_int):
        mod_list = []
        if mod_int & 1 << 0:   mod_list.append('NF')
        if mod_int & 1 << 1:   mod_list.append('EZ')
        if mod_int & 1 << 2:   mod_list.append('HD')
        if mod_int & 1 << 3:   mod_list.append('HR')
        if mod_int & 1 << 4:   mod_list.append('SD')
        if mod_int & 1 << 5:   mod_list.append('DT')
        if mod_int & 1 << 6:   mod_list.append('RX')
        if mod_int & 1 << 7:   mod_list.append('HT')
        if mod_int & 1 << 8:   mod_list.append('NC')
        if mod_int & 1 << 9:  mod_list.append('FL')
        if mod_int & 1 << 10:  mod_list.append('SO')
        if mod_int & 1 << 11:  mod_list.append('PF')
        if mod_int & 1 << 12:  mod_list.append('TD')

        return mod_list
