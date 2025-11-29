from connect_db import *
import csv
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed


# ---------------- CONFIG ----------------

NAME_BASICS_TSV = Path("C:\\My_Programs\\Temp\\Data\\name.basics.tsv")
BATCH_SIZE = 2000  # keep it modest; adjust if stable

# ----------------------------------------

def parse_int(value):
    # Return int
    if value is None or value == "" or value == r"\N":
        return None
    try:
        return int(value)
    except ValueError:
        return None


# ---------------- STEP 1: collect distinct professions ----------------

def collect_distinct_professions(tsv_path: Path):
    """
    First pass over name.basics.tsv:
    - Collect all distinct profession names from primaryProfession
      (splitting comma-separated lists).
    """
    professions = set()

    with tsv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in reader:
            prof_str = row.get("primaryProfession")
            if prof_str and prof_str != r"\N":
                for part in prof_str.split(","):
                    p = part.strip()
                    if p:
                        professions.add(p)

    return professions


def insert_professions(professions):
    """
    Insert distinct profession into lookup tables,
    """
    conn = connect_db()
    conn.autocommit = False
    cur = conn.cursor()

    # Insert professions
    if professions:
        insert_sql = """
            INSERT IGNORE INTO profession (profession_name) VALUES (%s);
        """
        cur.executemany(insert_sql, [(p,) for p in professions])

    conn.commit()

    cur.close()
    conn.close()


def lookups_professions():
    """
    look up profession from tables,
    """
    conn = connect_db()
    conn.autocommit = False
    cur = conn.cursor()

    # Build map: name -> id
    prof_map = {}
    cur.execute("SELECT id, profession_name FROM profession;")
    for pid, pname in cur.fetchall():
        prof_map[pname] = pid

    cur.close()
    conn.close()
    return prof_map

# -------- STEP 2: get existing titles for name_known_for FK safety --------

def load_existing_title_ids():
    """
    Load all existing tconst values from title_basics into a set.

    This ensures we only insert name_known_for rows where the referenced
    title actually exists, avoiding FK violations.
    """
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT tconst FROM title_basics;")
    title_ids = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return title_ids


# -------- STEP 3: load name_basics, person_profession, name_known_for --------

def load_name_basics_and_bridges(
    tsv_path: Path,
    profession_map,
    existing_title_ids,
    max_workers: int = 5,
    chunk_size: int = 10000,
):
    """
    Multi-threaded loader for name.basics.tsv:
    - Splits the file into chunks of rows.
    - Each chunk is processed in a separate thread with its own DB connection.
    - For each chunk, inserts into:
        * name_basics          (parent)
        * person_profession    (child of name_basics)
        * name_known_for       (child of name_basics and title_basics)
    Parameters
    ----------
    tsv_path : Path
        Path to name.basics.tsv
    profession_map : dict
        Maps profession_name -> profession_id (from 'profession' table)
    existing_title_ids : set
        Set of existing tconst values in title_basics (for FK safety)
    max_workers : int
        Number of worker threads to use
    chunk_size : int
        Number of TSV rows per chunk submitted to a worker
    """


    def process_chunk(rows):
        """
        Process a list of CSV rows (dicts) in a single thread:
        - Open a new DB connection
        - Build batches for:
            * name_basics
            * person_profession
            * name_known_for
        - Insert parents first (name_basics), then children
        """

        insert_name_basics_sql = """
            INSERT INTO name_basics (
                nconst,
                primaryName,
                birthYear,
                deathYear
            ) VALUES (%s, %s, %s, %s);
        """

        insert_person_profession_sql = """
            INSERT INTO person_profession (nconst, profession_id)
            VALUES (%s, %s);
        """

        insert_name_known_for_sql = """
            INSERT INTO name_known_for (nconst, tconst, position)
            VALUES (%s, %s, %s);
        """

        name_basics_batch = []
        person_prof_batch = []
        known_for_batch = []

        for row in rows:
            nconst = row.get("nconst")
            if not nconst:
                continue

            primaryName = None if row.get("primaryName") in (None, r"\N") else row["primaryName"]
            birthYear = parse_int(row.get("birthYear"))
            deathYear = parse_int(row.get("deathYear"))

            # Parent row: name_basics
            name_basics_batch.append((nconst, primaryName, birthYear, deathYear))

            # Child rows: person_profession (dedupe professions per person)
            prof_str = row.get("primaryProfession")
            if prof_str and prof_str != r"\N":
                prof_ids_for_person = set()
                for part in prof_str.split(","):
                    p = part.strip()
                    if not p:
                        continue
                    prof_id = profession_map.get(p)
                    if prof_id is not None:
                        prof_ids_for_person.add(prof_id)
                for prof_id in prof_ids_for_person:
                    person_prof_batch.append((nconst, prof_id))

            # Child rows: name_known_for (dedupe titles per person)
            kft_str = row.get("knownForTitles")
            if kft_str and kft_str != r"\N":
                titles = [t.strip() for t in kft_str.split(",") if t.strip()]
                seen_titles = set()
                pos = 0
                for tconst_known in titles:
                    if tconst_known in seen_titles:
                        continue
                    seen_titles.add(tconst_known)
                    if tconst_known in existing_title_ids:
                        pos += 1
                        known_for_batch.append((nconst, tconst_known, pos))

        try:

            conn = connect_db()
            conn.autocommit = False
            cur = conn.cursor()
            # Insert parents first
            if name_basics_batch:
                cur.executemany(insert_name_basics_sql, name_basics_batch)

            # Then insert children
            if person_prof_batch:
                cur.executemany(insert_person_profession_sql, person_prof_batch)

            if known_for_batch:
                cur.executemany(insert_name_known_for_sql, known_for_batch)

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

        return len(name_basics_batch), len(person_prof_batch), len(known_for_batch)

    # -------- Main body: read TSV, build chunks, submit to thread pool --------
    total_names = 0
    total_prof_links = 0
    total_known_for_links = 0
    futures = []

    with tsv_path.open("r", encoding="utf-8") as f, ThreadPoolExecutor(max_workers=max_workers) as executor:
        reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        current_chunk = []
        for i, row in enumerate(reader, start=1):

            current_chunk.append(row)

            if len(current_chunk) >= chunk_size:
                futures.append(executor.submit(process_chunk, current_chunk))
                current_chunk = []

            if i % 100000 == 0:
                print(f"Queued {i} rows from name.basics.tsv")

        # Submit any remaining rows
        if current_chunk:
            futures.append(executor.submit(process_chunk, current_chunk))

        # Collect results as chunks complete
        for fut in as_completed(futures):
            n_names, n_prof, n_known_for = fut.result()
            total_names += n_names
            total_prof_links += n_prof
            total_known_for_links += n_known_for

    print("Finished loading name_basics, person_profession, and name_known_for via threads.")
    print(f"Total name_basics rows inserted: {total_names}")
    print(f"Total person_profession rows inserted: {total_prof_links}")
    print(f"Total name_known_for rows inserted: {total_known_for_links}")



# ---------------- MAIN ----------------

def main():
    if not NAME_BASICS_TSV.exists():
        raise FileNotFoundError(f"TSV file not found: {NAME_BASICS_TSV}")

    print("Pass 1: collecting distinct professions...")
    professions = collect_distinct_professions(NAME_BASICS_TSV)
    print(f"Found {len(professions)} distinct profession values")

    print("Inserting/Updating lookup table 'profession'...")
    # insert_professions(professions)
    profession_map = lookups_professions()
    print("Profession lookup loaded.")

    print("Loading existing title IDs from 'title_basics' for knownFor FK safety...")
    existing_title_ids = load_existing_title_ids()
    print(f"Loaded {len(existing_title_ids)} existing title IDs")

    print("Pass 2: loading name_basics, person_profession, and name_known_for...")
    load_name_basics_and_bridges(NAME_BASICS_TSV, profession_map, existing_title_ids)

    print("All done for name.basics.tsv")


if __name__ == "__main__":
    main()
