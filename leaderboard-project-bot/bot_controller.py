from urllib.parse import urlparse

import ossapi

import psycopg2
import os
import csv
import io
from table2ascii import table2ascii as t2a, PresetStyle
from typing import List


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
    if mod_int & 1 << 0:
        mod_list.append('NF')
    if mod_int & 1 << 1:
        mod_list.append('EZ')
    if mod_int & 1 << 2:
        mod_list.append('HD')
    if mod_int & 1 << 3:
        mod_list.append('HR')
    if mod_int & 1 << 4:
        mod_list.append('SD')
    if mod_int & 1 << 6:
        mod_list.append('RX')
    if mod_int & 1 << 7:
        mod_list.append('HT')
    if mod_int & 1 << 8:
        mod_list.append('NC')
    elif mod_int & 1 << 5:
        mod_list.append('DT')
    if mod_int & 1 << 9:
        mod_list.append('FL')
    if mod_int & 1 << 10:
        mod_list.append('SO')
    if mod_int & 1 << 11:
        mod_list.append('PF')
    if mod_int & 1 << 12:
        mod_list.append('TD')

    return mod_list


def create_beatmap_query(min_length: int, max_length: int, min_stars: float, max_stars: float, min_ar: float,
                         max_ar: float, min_od: float, max_od: float, min_spinners: float, max_spinners: float,
                         tags: List[str], year: int
                         ):
    output_header = []
    beatmap_ids_query = """SELECT beatmap_id 
                                FROM beatmaps"""
    beatmap_query_params = []
    beatmap_query_args = {}

    if min_length is not None:
        beatmap_query_params.append(f"length >= %(min_length)s")
        beatmap_query_args['min_length'] = min_length
        output_header.append(f"length>={min_length}")
    if max_length is not None:
        beatmap_query_params.append(f"length <= %(max_length)s")
        beatmap_query_args['max_length'] = max_length
        output_header.append(f"length<={max_length}")
    if min_stars is not None:
        beatmap_query_params.append(f"difficulty_rating >= %(min_stars)s")
        beatmap_query_args['min_stars'] = min_stars
        output_header.append(f"stars>={min_stars}")
    if max_stars is not None:
        beatmap_query_params.append(f"difficulty_rating <= %(max_stars)s")
        beatmap_query_args['max_stars'] = max_stars
        output_header.append(f"stars<={max_stars}")
    if min_ar is not None:
        beatmap_query_params.append(f"ar >= %(min_ar)s")
        beatmap_query_args['min_ar'] = min_ar
        output_header.append(f"ar>={min_ar}")
    if max_ar is not None:
        beatmap_query_params.append(f"ar <= %(max_ar)s")
        beatmap_query_args['max_ar'] = max_ar
        output_header.append(f"ar<={max_ar}")
    if min_od is not None:
        beatmap_query_params.append(f"od >= %(min_od)s")
        beatmap_query_args['min_od'] = min_od
        output_header.append(f"od>={min_od}")
    if max_od is not None:
        beatmap_query_params.append(f"od <= %(max_od)s")
        beatmap_query_args['max_od'] = max_od
        output_header.append(f"od<={max_od}")
    if min_spinners is not None:
        beatmap_query_params.append(f"spinners >= %(min_spinners)s")
        beatmap_query_args['min_spinners'] = min_spinners
        output_header.append(f"spinners>={min_spinners}")
    if max_spinners is not None:
        beatmap_query_params.append(f"spinners <= %(max_spinners)s")
        beatmap_query_args['max_spinners'] = max_spinners
        output_header.append(f"spinners<={max_spinners}")
    if tags is not None:
        for index, tag in enumerate(tags):
            beatmap_query_params.append(f"%(tag{index})s = ANY(tags)")
            beatmap_query_args[f'tag{index}'] = tag
        output_header.append(f"tags={tags}")

    if year is not None:
        start_year = f"{year}-1-1"
        end_year = f"{year + 1}-1-1"
        beatmap_query_params.append(f"approved_date >= %(map_start_year)s AND approved_date <= %(map_end_year)s")
        beatmap_query_args[f'map_start_year'] = start_year
        beatmap_query_args[f'map_end_year'] = end_year
        output_header.append(f"map_year={year}")

    if len(beatmap_query_params) != 0:
        beatmap_ids_query += ' WHERE '
        beatmap_ids_query += ' AND '.join(beatmap_query_params)

    return beatmap_ids_query, beatmap_query_args, output_header


def create_score_query(mods: str, max_acc: float, min_acc: float, user_id: int, year: int, combine_mods: bool):
    score_query_params = []
    score_query_args = {}
    output_header = []
    odd_mods = ['PF', 'SD', 'NC']

    if mods is not None:
        mods = mods.upper()
        mod_list = [mods[i:i + 2] for i in range(0, len(mods), 2)]

        if combine_mods:
            cleaned_mods = [mod for mod in mod_list if mod not in odd_mods]
            if "NC" in mod_list:
                cleaned_mods.append("DT")
            # Add cleaned and all odd mod combinations to our query. This will add NC to scores that didn't have DT, but
            # that should be fine since NC should not be able to exist on its own.
            out_mods = (convert_mod_list_to_bitwise(cleaned_mods),)
            for mod in odd_mods:
                if (mod == 'NC' and 'DT' in mod_list) or mod != 'NC':
                    bitwise_mods = convert_mod_list_to_bitwise(cleaned_mods + [mod])
                    out_mods += (bitwise_mods,)

            score_query_params.append(f"mods IN %(mods)s")
            score_query_args['mods'] = out_mods
            extra_mods = f"SDPF{'NC' if 'DT' in cleaned_mods else ''}"
            output_header.append(f"mods={''.join(cleaned_mods)}({extra_mods})")

        else:
            out_mods = (convert_mod_list_to_bitwise(mod_list),)
            score_query_params.append(f"mods = %(mods)s")
            score_query_args['mods'] = out_mods
            output_header.append(f"mods={mods}")

    if min_acc is not None:
        score_query_params.append(f"accuracy >= %(min_acc)s")
        score_query_args['min_acc'] = min_acc
        output_header.append(f"acc>={min_acc}")
    if max_acc is not None:
        score_query_params.append(f"accuracy <= %(max_acc)s")
        score_query_args['max_acc'] = max_acc
        output_header.append(f"acc<={max_acc}")
    if user_id is not None:
        score_query_params.append(f"user_id = %(user_id)s")
        score_query_args['user_id'] = user_id
        output_header.append(f"user_id={user_id}")
    if year is not None:
        start_year = f"{year}-1-1"
        end_year = f"{year + 1}-1-1"
        score_query_params.append(f"created_at >= %(score_start_year)s AND created_at <= %(score_end_year)s")
        score_query_args[f'score_start_year'] = start_year
        score_query_args[f'score_end_year'] = end_year
        output_header.append(f"score_year={year}")

    return score_query_params, score_query_args, output_header


def retrieve_leaderboard(beatmap_id):
    conn = establish_conn()
    cursor = conn.cursor()
    query = f"SELECT * FROM scores WHERE beatmap_id = %(beatmap_id)s ORDER BY score DESC"
    cursor.execute(query, {'beatmap_id': beatmap_id})
    leaderboard = cursor.fetchall()
    leaderboard_header = ['Username', 'Score', 'Accuracy', 'Mods', 'PP']
    leaderboard_output = []
    for row in leaderboard[:50]:
        mods = convert_bitwise_to_mod_list(row[7])
        modstring = ""
        for mod in mods:
            modstring += mod
        leaderboard_output.append(
            [row[1], "{:,}".format(row[4]), str(round(row[5], 2)), modstring, str(round(row[12], 2))])
    conn.close()
    return t2a(header=leaderboard_header, body=leaderboard_output, style=PresetStyle.borderless)


def retrieve_beatmap_data(beatmap_id):
    conn = establish_conn()
    cursor = conn.cursor()
    query = f"SELECT * FROM beatmaps WHERE beatmap_id = %(beatmap_id)s"
    cursor.execute(query, {'beatmap_id': beatmap_id})
    beatmap_data = cursor.fetchone()
    conn.close()
    return beatmap_data


def retrieve_1s(mods: str, max_acc: float, min_acc: float, user_id: int, max_length: int, min_length: int,
                min_stars: float, max_stars: float, min_ar: float, max_ar: float, min_od: float, max_od: float,
                min_spinners: int, max_spinners: int, tags: List[str], map_year: int, score_year: int,
                combine_mods: bool):
    conn = establish_conn()
    cursor = conn.cursor()

    beatmap_ids_query, beatmap_query_args, beatmap_output_header = create_beatmap_query(min_length, max_length,
                                                                                        min_stars,
                                                                                        max_stars,
                                                                                        min_ar, max_ar, min_od,
                                                                                        max_od, min_spinners,
                                                                                        max_spinners, tags, map_year)

    score_query_params, score_query_args, score_output_header = create_score_query(mods, max_acc, min_acc, user_id,
                                                                                   score_year,
                                                                                   combine_mods)

    rank1s_query = f"""WITH ids AS ({beatmap_ids_query})
                        SELECT scores.*, beatmaps.* FROM 
                            scores
                        INNER JOIN beatmaps
                            ON scores.beatmap_id = beatmaps.beatmap_id
                        WHERE scores.beatmap_id IN (SELECT beatmap_id FROM ids) AND rank = 1"""

    if len(score_query_params) != 0:
        rank1s_query += ' AND '
        rank1s_query += ' AND '.join(score_query_params)

    beatmap_query_args.update(score_query_args)
    output_header = ', '.join(score_output_header + beatmap_output_header)

    cursor.execute(rank1s_query, beatmap_query_args)
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
                min_spinners: int, max_spinners: int, tags: List[str], map_year: int, score_year: int,
                combine_mods: bool):
    conn = establish_conn()
    cursor = conn.cursor()

    beatmap_ids_query, beatmap_query_args, beatmap_output_header = create_beatmap_query(min_length, max_length,
                                                                                        min_stars,
                                                                                        max_stars,
                                                                                        min_ar, max_ar, min_od,
                                                                                        max_od, min_spinners,
                                                                                        max_spinners, tags, map_year)

    score_query_params, score_query_args, score_output_header = create_score_query(mods, max_acc, min_acc, user_id,
                                                                                   score_year,
                                                                                   combine_mods)

    rank1s_query = f"""WITH ids AS ({beatmap_ids_query})
                    SELECT RANK () OVER (ORDER BY COUNT(*) DESC) AS rank, username, COUNT(*) FROM scores
                    WHERE scores.beatmap_id in (SELECT beatmap_id FROM ids) AND rank = 1
                    """
    beatmap_query_args.update(score_query_args)
    output_header = ', '.join(score_output_header + beatmap_output_header)

    if len(score_query_params) != 0:
        rank1s_query += ' AND '
        rank1s_query += ' AND '.join(score_query_params)

    rank1s_query += ' GROUP BY username ORDER BY COUNT(*) DESC'

    cursor.execute(rank1s_query, beatmap_query_args)
    leaderboard_output = cursor.fetchall()

    return leaderboard_output, output_header


def link_account(discord_id: int, osu_username: str):
    conn = establish_conn()
    query = f"""INSERT INTO users
                VALUES (%(discord_id)s, %(osu_username)s)
                ON CONFLICT (discord_id) 
                DO UPDATE SET osu_username = %(osu_username)s"""
    cursor = conn.cursor()
    cursor.execute(query, {'discord_id': discord_id, 'osu_username': osu_username})
    cursor.close()
    conn.commit()
    conn.close()


def check_account(discord_id: int):
    conn = establish_conn()
    query = f"""SELECT osu_username FROM users WHERE discord_id = {discord_id}"""
    cursor = conn.cursor()
    cursor.execute(query)
    osu_id = cursor.fetchone()
    cursor.close()
    conn.close()
    return osu_id


def user_stats_lookup(username: str):
    conn = establish_conn()
    cursor = conn.cursor()
    stats_dict = {}
    cursor.execute("SELECT COUNT(*) FROM scores WHERE username = %(username)s AND rank = 1", {'username': username})
    stats_dict['top1s'] = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM scores WHERE username = %(username)s AND rank <= 8", {'username': username})
    stats_dict['top8s'] = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM scores WHERE username = %(username)s AND rank <= 50", {'username': username})
    stats_dict['top50s'] = cursor.fetchone()[0]
    cursor.execute("SELECT mods, COUNT(*) FROM scores WHERE username = %(username)s AND rank = 1 GROUP BY mods",
                   {'username': username})
    stats_dict['mod_stats'] = cursor.fetchall()

    print(stats_dict)

    return stats_dict
