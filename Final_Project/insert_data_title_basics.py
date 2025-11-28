"""
Pre-process data and insert the normalized data from title_basics
"""
import csv
from pathlib import Path
from connect_db import *
from concurrent.futures import ThreadPoolExecutor, as_completed

TITLE_BASICS_TSV = Path("C:\\My_Programs\\Temp\\Data\\title.basics.tsv")
BATCH_SIZE = 2000


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

def insert(title_types, genres):
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

    cur.close()
    conn.close()


def lookups():
    """
    look up genres and title_types from tables,
    """
    conn = connect_db()
    conn.autocommit = False
    cur = conn.cursor()

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


def load_title_basics_and_title_genre(tsv_path: Path,
                                      title_type_map,
                                      genre_map,
                                      max_workers: int=5,
                                      chunk_size: int=10000
                                      ):

    """
    Multi-threaded loader for title.basics.tsv:
    - Splits the file into chunks of rows.
    - Each chunk is processed in a separate thread with its own DB connection.
    - For each chunk, inserts into title_basics (parent) and then title_genre (child).

    Parameters
    ----------
    tsv_path : Path
        Path to title.basics.tsv
    title_type_map : dict
        Maps cleaned titleType text -> title_type_id
    genre_map : dict
        Maps cleaned genre text -> genre_id
    max_workers : int
        Number of worker threads to use for DB insertions.
    chunk_size : int
        Number of TSV rows per chunk submitted to a worker.
    """

    def process_chunk(rows):
        """
        Process a list of CSV rows (dicts) in a single thread:
        - Open a new DB connection
        - Build batches for title_basics and title_genre
        - Insert parents first, then children
        """

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

        insert_title_genre_sql = """
            INSERT IGNORE INTO title_genre (tconst, genre_id)
             VALUES (%s, %s);
        """

        title_basics_batch = []
        title_genre_batch = []

        for row in rows:
            tconst = row.get("tconst")
            if not tconst:
                continue

            primaryTitle = None if row.get("primaryTitle") in (None, r"\N") else row["primaryTitle"]
            originalTitle = None if row.get("originalTitle") in (None, r"\N") else row["originalTitle"]
            isAdult = parse_bool_01(row.get("isAdult"))

            # Use range-aware parsing if you implemented it
            startYear = parse_int(row.get("startYear"))
            endYear = parse_int(row.get("endYear"))
            runtimeMinutes = parse_int(row.get("runtimeMinutes"))

            # Map titleType text -> title_type_id
            titleType_raw = row.get("titleType")
            title_type_id = None
            if titleType_raw and titleType_raw != r"\N":
                tt_clean = titleType_raw.strip()
                if tt_clean:
                    title_type_id = title_type_map.get(tt_clean)

            # Parent row
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

            # Children rows (genres)
            genres_raw = row.get("genres")
            if genres_raw and genres_raw != r"\N":
                for part in genres_raw.split(","):
                    g_clean = part.strip()
                    if not g_clean or g_clean == r"\N":
                        continue
                    genre_id = genre_map.get(g_clean)
                    if genre_id is not None:
                        title_genre_batch.append((tconst, genre_id))

        try:

            conn = connect_db()
            conn.autocommit = False
            cur = conn.cursor()

            # Insert parents first
            cur.executemany(insert_title_basics_sql, title_basics_batch)

            cur.executemany(insert_title_genre_sql, title_genre_batch)

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

        return len(title_basics_batch), len(title_genre_batch)

    # -------- Main function body: read TSV, submit chunks to threads --------
    total_titles = 0
    total_genres = 0
    futures = []

    with tsv_path.open("r", encoding="utf-8") as f, ThreadPoolExecutor(max_workers=max_workers) as executor:
        reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        current_chunk = []
        for i, row in enumerate(reader, start=1):
            current_chunk.append(row)

            if len(current_chunk) >= chunk_size:
                # Submit chunk to a worker thread
                futures.append(executor.submit(process_chunk, current_chunk))
                current_chunk = []

            if i % 100000 == 0:
                print(f"Queued {i} rows from title.basics.tsv")

        # Submit any remaining rows
        if current_chunk:
            futures.append(executor.submit(process_chunk, current_chunk))

        # Collect results
        for fut in as_completed(futures):
            inserted_titles, inserted_genres = fut.result()
            total_titles += inserted_titles
            total_genres += inserted_genres

    print(f"Finished loading title_basics and title_genre via threads.")
    print(f"Total title_basics rows inserted: {total_titles}")
    print(f"Total title_genre rows inserted: {total_genres}")

def main():

    print("Collecting distinct titleType and genres...")
    title_types, genres = collect_distinct_types_and_genres(TITLE_BASICS_TSV)
    print(f"Found {len(title_types)} distinct titleType values")
    print(f"Found {len(genres)} distinct genre values")

    print("Inserting lookup tables (title_type, genre)...")
    # insert(title_types, genres)
    title_type_map, genre_map = lookups()

    print("Loading title_basics and title_genre")
    load_title_basics_and_title_genre(TITLE_BASICS_TSV, title_type_map, genre_map)


if __name__ == '__main__':
    main()