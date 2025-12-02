from connect_db import *
import csv
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed


# ---------------- CONFIG ----------------

TITLE_AKAS_TSV = Path("C:\\My_Programs\\Temp\\Data\\title.akas.tsv")  # update as needed
BATCH_SIZE = 2000

# ----------------------------------------


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


# ---------------- STEP 1: collect distinct types & attributes ----------------


def collect_distinct_types_and_attributes(tsv_path: Path):
    """
    First pass over title.akas.tsv:
    - Collect all distinct 'types' values (splitting comma lists)
    - Collect all distinct 'attributes' values (splitting comma lists)
    """
    aka_types = set()
    aka_attributes = set()

    with tsv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in reader:
            types_str = row.get("types")
            if types_str and types_str != r"\N":
                for part in types_str.split(","):
                    t = part.strip()
                    if t:
                        aka_types.add(t)

            attrs_str = row.get("attributes")
            if attrs_str and attrs_str != r"\N":
                for part in attrs_str.split(","):
                    a = part.strip()
                    if a:
                        aka_attributes.add(a)

    return aka_types, aka_attributes


def insert_types_attributes(aka_types, aka_attributes):
    """
    Insert distinct types and attributes into their lookup tables
    """
    conn = connect_db()
    conn.autocommit = False
    cur = conn.cursor()

    # Insert into types
    if aka_types:

        insert_type_sql = """
            INSERT IGNORE INTO types (type_name) VALUES (%s);
                          """

        cur.executemany(insert_type_sql, [(t,) for t in aka_types])

    # Insert into title_attribute
    if aka_attributes:
        insert_attr_sql = """
            INSERT IGNORE INTO title_attribute (attribute_name) VALUES (%s);
                          """

        cur.executemany(insert_attr_sql, [(a,) for a in aka_attributes])

    conn.commit()

    cur.close()
    conn.close()


def lookup_types_attributes():
    """
    look up types and attributes from tables,
    build maps:
      - ype_name       -> type_id
      - attribute_name  -> attribute_id

    """
    conn = connect_db()
    conn.autocommit = False
    cur = conn.cursor()

    # Build maps
    aka_type_map = {}
    aka_attr_map = {}

    cur.execute("SELECT id, type_name FROM types;")
    for tid, tname in cur.fetchall():
        aka_type_map[tname] = tid

    cur.execute("SELECT id, attribute_name FROM title_attribute;")
    for aid, aname in cur.fetchall():
        aka_attr_map[aname] = aid

    cur.close()
    conn.close()
    return aka_type_map, aka_attr_map


# -------- STEP 2: get existing title IDs for FK safety --------


def load_existing_title_ids():
    """
    Load existing tconst values from title_basics into a set.
    This avoids FK errors: we only insert akas for titles we actually have.
    """
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT tconst FROM title_basics;")
    title_ids = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return title_ids


# -------- STEP 3: load title_akas and bridge tables --------


def load_title_akas_and_bridges(
    tsv_path: Path,
    aka_type_map,
    aka_attr_map,
    existing_title_ids,
    max_workers: int = 4,
    chunk_size: int = 10000,
):
    """
    Multi-threaded loader for title.akas.tsv (using surrogate PK aka_id):

    - Splits the TSV into chunks of rows (dicts).
    - Each chunk is processed in its own thread with its own DB connection.
    - For each row in a chunk:
        1) Insert into title_akas (one row, get aka_id via lastrowid)
        2) Insert rows into:
             * title_aka_type(aka_id, aka_type_id)
             * title_aka_attribute(aka_id, aka_attribute_id)

    Assumptions:
    - title_akas has: aka_id INT AUTO_INCREMENT PRIMARY KEY, plus the other fields.
    - title_aka_type and title_aka_attribute use aka_id as FK.
    - existing_title_ids is a set of valid tconst values from title_basics (FK safety).
    - Global constants BATCH_SIZE and MAX_ROWS may be defined elsewhere.
    """

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def process_chunk(rows):
        """
        Process a list of CSV rows (dicts) in a single thread:
        - Open a new DB connection
        - Insert into title_akas row-by-row (to capture aka_id)
        - Batch insert into title_aka_type and title_aka_attribute using aka_id
        """
        conn = connect_db()
        conn.autocommit = False
        cur = conn.cursor()

        insert_title_akas_sql = """
            INSERT INTO title_akas (
                titleId,
                ordering,
                title,
                region_code,
                language_code,
                isOriginalTitle
            ) VALUES (%s, %s, %s, %s, %s, %s);
        """

        insert_title_aka_type_sql = """
            INSERT INTO title_aka_type (
                title_akas_id,
                title_types_id
            ) VALUES (%s, %s);
        """

        insert_title_aka_attr_sql = """
            INSERT INTO title_aka_attribute (
                title_akas_id,
                title_attribute_id
            ) VALUES (%s, %s);
        """

        aka_type_batch = []
        aka_attr_batch = []
        aka_count = 0

        try:
            for row in rows:
                titleId = row.get("titleId")
                if not titleId:
                    continue

                # Only keep akas for titles that exist in title_basics
                if titleId not in existing_title_ids:
                    continue

                ordering_raw = row.get("ordering")
                try:
                    ordering = int(ordering_raw) if ordering_raw not in (None, "", r"\N") else None
                except ValueError:
                    ordering = None

                if ordering is None:
                    # ordering is logically important; skip malformed rows
                    continue

                title = None if row.get("title") in (None, r"\N") else row["title"]
                region_code = None if row.get("region") in (None, r"\N") else row["region"]
                language_code = None if row.get("language") in (None, r"\N") else row["language"]
                isOriginalTitle = parse_bool_01(row.get("isOriginalTitle"))

                # ---- 1) Insert into core table title_akas; get aka_id ----
                cur.execute(
                    insert_title_akas_sql,
                    (titleId, ordering, title, region_code, language_code, isOriginalTitle),
                )
                aka_id = cur.lastrowid
                aka_count += 1

                # ---- 2) Build bridge rows using aka_id as FK ----

                # types -> title_aka_type
                types_str = row.get("types")
                if types_str and types_str != r"\N":
                    for part in types_str.split(","):
                        t = part.strip()
                        if not t:
                            continue
                        type_id = aka_type_map.get(t)
                        if type_id is not None:
                            aka_type_batch.append((aka_id, type_id))

                # attributes -> title_aka_attribute
                attrs_str = row.get("attributes")
                if attrs_str and attrs_str != r"\N":
                    for part in attrs_str.split(","):
                        a = part.strip()
                        if not a:
                            continue
                        attr_id = aka_attr_map.get(a)
                        if attr_id is not None:
                            aka_attr_batch.append((aka_id, attr_id))

                # Optionally flush bridge batches inside the chunk if they get big
                if len(aka_type_batch) >= BATCH_SIZE or len(aka_attr_batch) >= BATCH_SIZE:
                    cur.executemany(insert_title_aka_type_sql, aka_type_batch)
                    aka_type_batch.clear()

                    cur.executemany(insert_title_aka_attr_sql, aka_attr_batch)
                    aka_attr_batch.clear()

            # Final flush for this chunk
            if aka_type_batch:
                cur.executemany(insert_title_aka_type_sql, aka_type_batch)
                aka_type_batch.clear()

            if aka_attr_batch:
                cur.executemany(insert_title_aka_attr_sql, aka_attr_batch)
                aka_attr_batch.clear()

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

        return aka_count

    # -------- Main body: read TSV, build chunks, submit to thread pool --------
    total_akas = 0
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
                print(f"Queued {i} rows from title.akas.tsv")

        # Submit remaining rows in the final chunk
        if current_chunk:
            futures.append(executor.submit(process_chunk, current_chunk))

        # Collect results from worker threads
        for fut in as_completed(futures):
            aka_count = fut.result()
            total_akas += aka_count

    print("Finished loading title_akas (aka_id PK), title_aka_type, and title_aka_attribute via threads.")
    print(f"Total title_akas rows inserted: {total_akas}")


# ---------------- MAIN ----------------


def main():
    if not TITLE_AKAS_TSV.exists():
        raise FileNotFoundError(f"TSV file not found: {TITLE_AKAS_TSV}")

    print("Pass 1: collecting distinct aka types and attributes...")
    aka_types, aka_attributes = collect_distinct_types_and_attributes(TITLE_AKAS_TSV)
    print(f"Found {len(aka_types)} distinct 'types' values")
    print(f"Found {len(aka_attributes)} distinct 'attributes' values")

    print("Inserting/Updating lookup tables aka_type and aka_attribute...")
    # insert_types_attributes(aka_types, aka_attributes)
    aka_type_map, aka_attr_map = lookup_types_attributes()
    print("Lookup tables loaded.")

    print("Loading existing title IDs from title_basics for FK safety...")
    existing_title_ids = load_existing_title_ids()
    print(f"Loaded {len(existing_title_ids)} existing title IDs")

    print("Pass 2: loading title_akas and bridge tables...")
    load_title_akas_and_bridges(
        TITLE_AKAS_TSV,
        aka_type_map,
        aka_attr_map,
        existing_title_ids,
    )

    print("All done for title.akas.tsv")


if __name__ == "__main__":
    main()
