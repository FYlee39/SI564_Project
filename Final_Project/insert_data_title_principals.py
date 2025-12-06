from connect_db import *
import csv
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------- CONFIG ----------------

TITLE_PRINCIPALS_TSV = Path("C:\\My_Programs\\Temp\\Data\\title.principals.tsv")  # update as needed
BATCH_SIZE = 200

# ----------------------------------------


# ---------------- STEP 1: collect distinct categories ----------------


def collect_distinct_categories(tsv_path: Path):
    """
    First pass over title.principals.tsv:
    Collect all distinct 'category' values (actor, director, producer, etc.).
    """
    categories = set()

    with tsv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in reader:
            cat = row.get("category")
            if cat and cat != r"\N":
                c = cat.strip()
                if c:
                    categories.add(c)

    return categories

def insert_categories(categories):
    """
    Insert distinct categories into principal_category looku
    """
    conn = connect_db()
    conn.autocommit = False
    cur = conn.cursor()

    if categories:
        insert_sql = """
                     INSERT IGNORE INTO principal_category (category_name) 
                     VALUES (%s);
                     """
        cur.executemany(insert_sql, [(c,) for c in categories])
        conn.commit()

def lookup_categories():
    """
    build a dict: category_name -> category_id.
    """
    conn = connect_db()
    conn.autocommit = False
    cur = conn.cursor()
    # Build map: name -> id
    cat_map = {}
    cur.execute("SELECT id, category_name FROM principal_category;")
    for cid, cname in cur.fetchall():
        cat_map[cname] = cid

    cur.close()
    conn.close()
    return cat_map


# -------- STEP 2: get existing title IDs for FK safety --------


def load_existing_title_ids():
    """
    Load existing tconst values from title_basics for FK safety.
    """
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT tconst FROM title_basics;")
    title_ids = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return title_ids


def load_existing_name_ids():
    """
    Load existing nconst values from name_basics for FK safety.
    """
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT nconst FROM name_basics;")
    name_ids = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return name_ids


# -------- STEP 3: load title_principals_and_characters tables --------


def load_title_principals_and_characters_mt(
        tsv_path: Path,
        category_map,
        existing_title_ids,
        existing_name_ids,
        max_workers: int=5,
        chunk_size: int=200,
):
    """
    Multi-threaded loader for title.principals.tsv (using surrogate PK 'id').

    Schema assumptions:
      - title_principals has:
            id          INT AUTO_INCREMENT PRIMARY KEY
            tconst      VARCHAR(...)
            ordering    INT
            nconst      VARCHAR(...)
            category_id INT NULL
            job         VARCHAR(...) NULL

      - principal_character has:
            principal_id   INT NOT NULL  -- FK → title_principals.id
            character_name VARCHAR(...)

    This function:
      - Splits the TSV into chunks of rows.
      - Each chunk is processed in a separate thread with its own DB connection.
      - Per row in a chunk:
          1) Insert into title_principals (single row, grab 'id' via lastrowid)
          2) Insert related characters into principal_character using that id.
    """

    def parse_characters_field(characters_raw: str):
        """
        Simple character parsing helper.

        IMDb often stores characters as JSON-like strings, e.g.:
          '["John Doe","Agent Smith"]'
        For this project we:
          - strip enclosing brackets if present
          - split on commas
          - trim quotes and whitespace
          - deduplicate while preserving order
        """
        if not characters_raw or characters_raw == r"\N":
            return []

        s = characters_raw.strip()
        if s.startswith("[") and s.endswith("]"):
            s = s[1:-1]

        parts = s.split(",")
        chars = []
        for p in parts:
            c = p.strip().strip('"').strip("'")
            if c:
                chars.append(c)

        seen = set()
        result = []
        for c in chars:
            if c not in seen:
                seen.add(c)
                result.append(c)
        return result

    def process_chunk(rows):
        """
        Process a list of CSV rows (dicts) in a single thread:
        - Open a DB connection
        - Insert each principal row one-by-one to get its 'id'
        - Batch insert character rows using that 'id' as FK
        """
        conn = connect_db()
        conn.autocommit = False
        cur = conn.cursor()

        insert_principal_sql = """
            INSERT INTO title_principals (
                tconst,
                ordering,
                nconst,
                category_id,
                job
            ) VALUES (%s, %s, %s, %s, %s);
        """

        insert_character_sql = """
            INSERT INTO principal_character (
                title_principals_id,
                character_name
            ) VALUES (%s, %s);
        """

        characters_batch = []
        principals_count = 0
        characters_count = 0

        try:
            for row in rows:
                tconst = row.get("tconst")
                if not tconst:
                    continue
                if tconst not in existing_title_ids:
                    # FK safety: only keep principals for titles we have
                    continue

                ordering_raw = row.get("ordering")
                try:
                    ordering = int(ordering_raw) if ordering_raw not in (None, "", r"\N") else None
                except ValueError:
                    ordering = None
                if ordering is None:
                    # ordering is logically required; skip malformed
                    continue

                nconst = row.get("nconst")
                if not nconst or nconst not in existing_name_ids:
                    # ensure person exists
                    continue

                category_raw = row.get("category")
                category_id = None
                if category_raw and category_raw != r"\N":
                    c = category_raw.strip()
                    if c:
                        category_id = category_map.get(c)

                job = row.get("job")
                if job in (None, r"\N"):
                    job = None

                # ---- 1) Insert into title_principals, get surrogate PK 'id' ----
                cur.execute(
                    insert_principal_sql,
                    (tconst, ordering, nconst, category_id, job),
                )
                conn.commit()
                principal_id = cur.lastrowid
                principals_count += 1

                # ---- 2) Parse characters and queue principal_character rows ----
                chars_raw = row.get("characters")
                char_list = parse_characters_field(chars_raw)
                for char_name in char_list:
                    characters_batch.append((principal_id, char_name))
                    characters_count += 1

                # Flush character batch if large
                if len(characters_batch) >= BATCH_SIZE:
                    cur.executemany(insert_character_sql, characters_batch)
                    characters_batch.clear()
                    conn.commit()

            # Final flush for leftover character rows in this chunk
            if characters_batch:
                cur.executemany(insert_character_sql, characters_batch)
                characters_batch.clear()
                conn.commit()

        except Exception as e:
            print(e)
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

        return principals_count, characters_count

    # -------- Main body: read TSV, chunk it, and dispatch to threads --------
    total_principals = 0
    total_characters = 0
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
                print(f"Queued {i} rows from title.principals.tsv")

        # Submit any remaining rows
        if current_chunk:
            futures.append(executor.submit(process_chunk, current_chunk))

        from concurrent.futures import as_completed
        for fut in as_completed(futures):
            p_count, c_count = fut.result()
            total_principals += p_count
            total_characters += c_count

    print("Finished loading title_principals (with id PK) and principal_character via threads.")
    print(f"Total title_principals rows inserted: {total_principals}")
    print(f"Total principal_character rows inserted: {total_characters}")


def main():
    if not TITLE_PRINCIPALS_TSV.exists():
        raise FileNotFoundError(f"TSV file not found: {TITLE_PRINCIPALS_TSV}")

    print("Pass 1: collecting distinct categories...")
    categories = collect_distinct_categories(TITLE_PRINCIPALS_TSV)
    print(f"Found {len(categories)} distinct category values")

    print("Inserting/Updating lookup table 'principal_category'...")
    insert_categories(categories)
    category_map = lookup_categories()
    print("Category lookup loaded.")

    print("Loading existing title IDs from 'title_basics'...")
    existing_title_ids = load_existing_title_ids()
    print(f"Loaded {len(existing_title_ids)} title IDs")

    print("Loading existing name IDs from 'name_basics'...")
    existing_name_ids = load_existing_name_ids()
    print(f"Loaded {len(existing_name_ids)} name IDs")

    print("Pass 2 (multi-threaded): loading title_principals and principal_character...")
    load_title_principals_and_characters_mt(
        TITLE_PRINCIPALS_TSV,
        category_map,
        existing_title_ids,
        existing_name_ids
    )

    print("All done for title.principals.tsv")


if __name__ == "__main__":
    main()
