from urllib.parse import urlparse

import psycopg2
import os
import csv
import io
from table2ascii import table2ascii as t2a, PresetStyle


def establish_conn():
    db_url = urlparse(os.environ['DATABASE_URL'])
    username = db_url.username
    password = db_url.password
    database = db_url.path[1:]
    hostname = db_url.hostname
    port = db_url.port
    try:
        conn = psycopg2.connect(database=database,
                                user=username,
                                password=password,
                                host=hostname,
                                )
        return conn
    except Exception as e:
        print(e)


def convert_mod_list_to_bitwise(mods):
    mod_int = 0
    for mod in mods:
        mod = mod.upper()
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


def convert_bitwise_to_mod_list(mod_int):
    mod_list = []
    if mod_int & 1 << 0:   mod_list.append('NF')
    if mod_int & 1 << 1:   mod_list.append('EZ')
    if mod_int & 1 << 2:   mod_list.append('HD')
    if mod_int & 1 << 3:   mod_list.append('HR')
    if mod_int & 1 << 4:   mod_list.append('SD')
    if mod_int & 1 << 6:   mod_list.append('RX')
    if mod_int & 1 << 7:   mod_list.append('HT')
    if mod_int & 1 << 8:
        mod_list.append('NC')
    elif mod_int & 1 << 5:
        mod_list.append('DT')
    if mod_int & 1 << 9:   mod_list.append('FL')
    if mod_int & 1 << 10:  mod_list.append('SO')
    if mod_int & 1 << 11:  mod_list.append('PF')
    if mod_int & 1 << 12:  mod_list.append('TD')

    return mod_list


def retrieve_leaderboard(beatmap_id):
    conn = establish_conn()
    cursor = conn.cursor()
    query = f"SELECT * FROM scores WHERE beatmap_id = {beatmap_id} ORDER BY score DESC"
    cursor.execute(query)
    leaderboard = cursor.fetchall()
    leaderboard_header = ['Username', 'Score', 'Accuracy', 'Mods', 'PP']
    leaderboard_output = []
    for row in leaderboard[:50]:
        mods = convert_bitwise_to_mod_list(row[7])
        modstring = ""
        for mod in mods:
            modstring += mod
        leaderboard_output.append(
            [row[1], "{:,}".format(row[4]), str(round(row[5], 2)), modstring, str(round(row[11], 2))])
    conn.close()
    return t2a(header=leaderboard_header, body=leaderboard_output, style=PresetStyle.borderless)


def retrieve_beatmap_data(beatmap_id):
    conn = establish_conn()
    cursor = conn.cursor()
    query = f"SELECT * FROM beatmaps WHERE beatmap_id = {beatmap_id}"
    cursor.execute(query)
    beatmap_data = cursor.fetchone()
    print(beatmap_data)
    conn.close()
    return beatmap_data


def retrieve_1s(mods: str, max_acc: float, min_acc: float, user_id: int, max_length: int, min_length: int,
                min_stars: float, max_stars: float, min_ar: float, max_ar: float, min_od: float, max_od: float,
                min_spinners: int, max_spinners: int, tag: str, combine_mods: bool):
    conn = establish_conn()
    cursor = conn.cursor()

    beatmap_ids_query = """SELECT beatmap_id 
                            FROM beatmaps"""
    beatmap_query_params = []

    if max_length is not None:
        beatmap_query_params.append(f"length <= {max_length}")
    if min_length is not None:
        beatmap_query_params.append(f"length >= {min_length}")
    if max_stars is not None:
        beatmap_query_params.append(f"difficulty_rating <= {max_stars}")
    if min_stars is not None:
        beatmap_query_params.append(f"difficulty_rating >= {min_stars}")
    if min_ar is not None:
        beatmap_query_params.append(f"ar >= {min_ar}")
    if max_ar is not None:
        beatmap_query_params.append(f"ar <= {max_ar}")
    if min_od is not None:
        beatmap_query_params.append(f"od >= {min_od}")
    if max_od is not None:
        beatmap_query_params.append(f"od <= {max_od}")
    if min_spinners is not None:
        beatmap_query_params.append(f"spinners >= {min_spinners}")
    if max_spinners is not None:
        beatmap_query_params.append(f"spinners <= {max_spinners}")

    if len(beatmap_query_params) != 0:
        beatmap_ids_query += ' WHERE '
        beatmap_ids_query += ' AND '.join(beatmap_query_params)

    rank1s_query = f"""WITH ids AS ({beatmap_ids_query})
                    SELECT scores.*, beatmaps.* FROM 
                        scores
                    INNER JOIN beatmaps
                        ON scores.beatmap_id = beatmaps.beatmap_id
                    WHERE scores.beatmap_id in (SELECT beatmap_id FROM ids) AND rank = 1"""
    score_query_params = []

    if mods is not None:
        mod_list = [mods[i:i + 2] for i in range(0, len(mods), 2)]
        if combine_mods:
            mod_int_primary = convert_mod_list_to_bitwise(mod_list)
            for index, mod in enumerate(mod_list):
                if mod == 'dt' or mod == 'DT':
                    mod_list[index] = 'NC'
                    print(mod_list)
                elif mod == 'nc' or mod == 'NC':
                    mod_list[index] = 'DT'
                    print(mod_list)

            mod_int_secondary = convert_mod_list_to_bitwise(mod_list)

            score_query_params.append(f"mods IN ({mod_int_primary}, {mod_int_secondary})")

        else:
            mod_int = convert_mod_list_to_bitwise(mod_list)
            score_query_params.append(f"mods = {mod_int}")
    if max_acc is not None:
        score_query_params.append(f"accuracy <= {max_acc}")
    if min_acc is not None:
        score_query_params.append(f"accuracy >= {min_acc}")
    if user_id is not None:
        score_query_params.append(f"user_id = {user_id}")

    if len(score_query_params) != 0:
        rank1s_query += ' AND '
        rank1s_query += ' AND '.join(score_query_params)

    print(rank1s_query)
    cursor.execute(rank1s_query)
    rank1_data = cursor.fetchall()
    csv_header = ['Username', 'Beatmap ID', 'Artist', 'Title', 'Difficulty', 'Star Rating', 'Accuracy', 'Mods',
                  'Length', 'Max Combo', 'Spinners']
    csv_data = []
    for row in rank1_data:
        modstring = ""
        mods = convert_bitwise_to_mod_list(row[7])
        for mod in mods:
            modstring += mod

        csv_data.append(
            [row[1], row[3], row[19], row[18], row[21], row[22], row[5], modstring, row[28], row[29], row[32]])

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(csv_header)
    writer.writerows(csv_data)
    buffer.seek(0)
    conn.close()
    return buffer


def leaderboard(mods: str, max_acc: float, min_acc: float, user_id: int, max_length: int, min_length: int,
                min_stars: float, max_stars: float, min_ar: float, max_ar: float, min_od: float, max_od: float,
                min_spinners: int, max_spinners: int, tag: str, combine_mods: bool):
    conn = establish_conn()
    cursor = conn.cursor()

    beatmap_ids_query = """SELECT beatmap_id 
                                FROM beatmaps"""
    beatmap_query_params = []

    if max_length is not None:
        beatmap_query_params.append(f"length <= {max_length}")
    if min_length is not None:
        beatmap_query_params.append(f"length >= {min_length}")
    if max_stars is not None:
        beatmap_query_params.append(f"difficulty_rating <= {max_stars}")
    if min_stars is not None:
        beatmap_query_params.append(f"difficulty_rating >= {min_stars}")
    if min_ar is not None:
        beatmap_query_params.append(f"ar >= {min_ar}")
    if max_ar is not None:
        beatmap_query_params.append(f"ar <= {max_ar}")
    if min_od is not None:
        beatmap_query_params.append(f"od >= {min_od}")
    if max_od is not None:
        beatmap_query_params.append(f"od <= {max_od}")
    if min_spinners is not None:
        beatmap_query_params.append(f"spinners >= {min_spinners}")
    if max_spinners is not None:
        beatmap_query_params.append(f"spinners <= {max_spinners}")
    if tag is not None:
        beatmap_query_params.append(f"{tag} = ANY (tags)")

    if len(beatmap_query_params) != 0:
        beatmap_ids_query += ' WHERE '
        beatmap_ids_query += ' AND '.join(beatmap_query_params)

    rank1s_query = f"""WITH ids AS ({beatmap_ids_query})
                    SELECT RANK () OVER (ORDER BY COUNT(*) DESC) AS rank, username, COUNT(*) FROM scores
                    WHERE scores.beatmap_id in (SELECT beatmap_id FROM ids) AND rank = 1
                    """
    score_query_params = []

    if mods is not None:
        mod_list = [mods[i:i + 2] for i in range(0, len(mods), 2)]

        if combine_mods:
            mod_int_primary = convert_mod_list_to_bitwise(mod_list)
            for index, mod in enumerate(mod_list):
                if mod == 'dt' or mod == 'DT':
                    mod_list[index] = 'NC'
                    print(mod_list)
                elif mod == 'nc' or mod == 'NC':
                    mod_list[index] = 'DT'
                    print(mod_list)

            mod_int_secondary = convert_mod_list_to_bitwise(mod_list)

            score_query_params.append(f"mods IN ({mod_int_primary}, {mod_int_secondary})")

        else:
            mod_int = convert_mod_list_to_bitwise(mod_list)
            score_query_params.append(f"mods = {mod_int}")
    if max_acc is not None:
        score_query_params.append(f"accuracy <= {max_acc}")
    if min_acc is not None:
        score_query_params.append(f"accuracy >= {min_acc}")
    if user_id is not None:
        score_query_params.append(f"user_id = {user_id}")

    if len(score_query_params) != 0:
        rank1s_query += ' AND '
        rank1s_query += ' AND '.join(score_query_params)

    rank1s_query += ' GROUP BY username ORDER BY COUNT(*) DESC'
    print(rank1s_query)
    cursor.execute(rank1s_query)
    leaderboard_output = cursor.fetchall()

    return leaderboard_output


def link_account(discord_id: int, osu_id: int):
    conn = establish_conn()
    query = f"""INSERT INTO users
                VALUES ({discord_id}, {osu_id})
                ON CONFLICT (discord_id) DO UPDATE SET osu_user_id = {osu_id}"""
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()
    conn.commit()
