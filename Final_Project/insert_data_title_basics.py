"""
Pre-process data and insert the normalized data from title_basics
"""
import mysql.connector as mysql
import csv
from collections import defaultdict
from pathlib import Path
from connect_db import *

TITLE_BASICS_TSV = Path("C:\\My_Programs\\Temp\\Data\\title.basics.tsv")
BATCH_SIZE = 200


def parse_int(value):
    # Return int
    if value is None or value == "" or value == r"\N":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_bool_01(value):
    # Return bool
    if value is None or value == "" or value == r"\N":
        return None
    return 1 if value == "1" else 0


def collect_distinct_types_and_genres(tsv_path: Path):
    """
    Collect distinct types and genres from a TSV file.
    - collect distinct titleType values
    - collect distinct genre values (splitting comma lists)
    """
    title_types = set()  # Distinct values
    genres = set()  # Distinct values

    with tsv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in reader:
            # Get title type
            tt = row.get("titleType")
            if tt and tt != r"\N":
                # If not null
                tt_clean = tt.strip()
                if tt_clean:
                    title_types.add(tt_clean)

            # Get genres (multiple values seperated by comma)
            g = row.get("genres")
            if g and g != r"\N":
                # If not null
                for part in g.split(","):
                    g_clean = part.strip()
                    if g_clean and g_clean != r"\N":
                        genres.add(g_clean)

    return title_types, genres


def insert_lookups(title_types, genres):
    """
    Insert distinct title_types & genres into lookup tables,
    """
    conn = connect_db()
    conn.autocommit = False
    cur = conn.cursor()

    # Insert title_type
    if title_types:
        insert_title_type_sql = """
            INSERT IGNORE INTO title_type (title_type_name) VALUES (%s);
        """
        cur.executemany(insert_title_type_sql, [(tt,) for tt in title_types])

    # Insert genre
    if genres:
        insert_genre_sql = """
            INSERT IGNORE INTO genre (genre_name) VALUES (%s);
        """
        cur.executemany(insert_genre_sql, [(g,) for g in genres])

    conn.commit()

    # Build maps from text -> id
    title_type_map = {}
    genre_map = {}

    cur.execute("SELECT title_type_id, title_type_name FROM title_type;")
    for tt_id, tt_name in cur.fetchall():
        title_type_map[tt_name] = tt_id

    cur.execute("SELECT genre_id, genre_name FROM genre;")
    for g_id, g_name in cur.fetchall():
        genre_map[g_name] = g_id

    cur.close()
    conn.close()
    return title_type_map, genre_map


def load_title_basics_and_title_genre(tsv_path: Path, title_type_map, genre_map):
    """
    - Insert cleaned rows into the normalized 'title_basics' table.
    - Insert exploded genre values into the 'title_genre' bridge table.
    """

    conn = connect_db()
    conn.autocommit = False
    cur = conn.cursor()

    # SQL for inserting into title_basics
    insert_title_basics_sql = """
        INSERT IGNORE INTO title_basics (
            tconst,
            primaryTitle,
            originalTitle,
            isAdult,
            startYear,
            endYear,
            runtimeMinutes,
            title_type_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    # SQL for inserting into the many-to-many table title_genre
    insert_title_genre_sql = """
        INSERT IGNORE INTO title_genre (tconst, genre_id)
        VALUES (%s, %s);
    """

    # Batches for efficient bulk insertion
    title_basics_batch = []
    title_genre_batch = []

    # Open and scan the TSV file again
    with tsv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)

        for i, row in enumerate(reader, start=1):

            # -------------------------------
            # 1. Extract and clean main fields
            # -------------------------------
            tconst = row.get("tconst")
            if not tconst:
                continue  # Required field missing; skip row

            primaryTitle = None if row.get("primaryTitle") in (None, r"\N") else row["primaryTitle"]
            originalTitle = None if row.get("originalTitle") in (None, r"\N") else row["originalTitle"]

            # Convert booleans, ints, etc.
            isAdult = parse_bool_01(row.get("isAdult"))
            startYear = parse_int(row.get("startYear"))
            endYear = parse_int(row.get("endYear"))
            runtimeMinutes = parse_int(row.get("runtimeMinutes"))

            # ----------------------------------------
            # 2. Map titleType text -> title_type_id FK
            # ----------------------------------------
            titleType_raw = row.get("titleType")
            title_type_id = None
            if titleType_raw and titleType_raw != r"\N":
                tt_clean = titleType_raw.strip()
                if tt_clean:
                    # If lookup doesn't exist, it's left as NULL (rare)
                    title_type_id = title_type_map.get(tt_clean)

            # Add to the title_basics batch
            title_basics_batch.append(
                (
                    tconst,
                    primaryTitle,
                    originalTitle,
                    isAdult,
                    startYear,
                    endYear,
                    runtimeMinutes,
                    title_type_id,
                )
            )

            # ---------------------------------------------------
            # 3. Handle multivalue "genres" → title_genre bridge
            # ---------------------------------------------------
            genres_raw = row.get("genres")
            if genres_raw and genres_raw != r"\N":
                # Split comma-separated genres
                for part in genres_raw.split(","):
                    g_clean = part.strip()
                    if not g_clean or g_clean == r"\N":
                        continue

                    # Map genre text -> genre_id
                    genre_id = genre_map.get(g_clean)
                    if genre_id is not None:
                        title_genre_batch.append((tconst, genre_id))

            # ---------------------------------------------------
            # 4. Periodically flush batches into the database
            # ---------------------------------------------------
            if len(title_basics_batch) >= BATCH_SIZE or len(title_genre_batch) >= BATCH_SIZE:
                cur.executemany(insert_title_basics_sql, title_basics_batch)
                title_basics_batch.clear()
                cur.executemany(insert_title_genre_sql, title_genre_batch)
                title_genre_batch.clear()
                conn.commit()

            # Progress message every 100k lines
            if i % 100000 == 0:
                print(f"Processed {i} rows from title.basics.tsv")

    # -----------------------------------
    # 5. Final flush after file is done
    # -----------------------------------
    if title_basics_batch:
        cur.executemany(insert_title_basics_sql, title_basics_batch)
    if title_genre_batch:
        cur.executemany(insert_title_genre_sql, title_genre_batch)
    conn.commit()

    cur.close()
    conn.close()
    print("Finished loading title_basics and title_genre.")


def main():

    print("Collecting distinct titleType and genres...")
    title_types, genres = collect_distinct_types_and_genres(TITLE_BASICS_TSV)
    print(f"Found {len(title_types)} distinct titleType values")
    print(f"Found {len(genres)} distinct genre values")

    print("Inserting lookup tables (title_type, genre)...")
    title_type_map, genre_map = insert_lookups(title_types, genres)

    print("Loading title_basics and title_genre")
    load_title_basics_and_title_genre(TITLE_BASICS_TSV, title_type_map, genre_map)


if __name__ == '__main__':
    main()