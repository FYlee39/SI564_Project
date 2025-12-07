from connect_db import *
import csv
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------- CONFIG ----------------

TITLE_CREW_TSV = Path("C:\\My_Programs\\Temp\\Data\\title.crew.tsv")
BATCH_SIZE = 2000

# ----------------------------------------


# ---------------- STEP 1: get existing title IDs and name IDs for FK safety ----------------


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


def load_title_crew_mt(
    tsv_path: Path,
    existing_title_ids,
    existing_name_ids,
    max_workers: int=5,
    chunk_size: int=1000,
        ):
    """
    Multi-threaded loader for title.crew.tsv:

    Raw file columns:
      - tconst
      - directors  (comma-separated list of nconst or NA)
      - writers    (comma-separated list of nconst or NA)

    Target normalized tables:
      - title_director(tconst, nconst)
      - title_writer(tconst, nconst)

    Both tables:
      - PK(id)
      - FK(tconst → title_basics.tconst)
      - FK(nconst → name_basics.nconst)

    Strategy:
      - Read TSV in chunks of rows.
      - Each chunk is processed in its own thread with its own DB connection.
      - Per chunk:
          * collect director pairs (tconst, nconst)
          * collect writer pairs  (tconst, nconst)
          * batch insert into title_director / title_writer
    """

    def process_chunk(rows):
        """
        Process a list of CSV rows in a single thread:
        - Build director and writer batches
        - Insert into title_director and title_writer
        """

        insert_director_sql = """
            INSERT INTO title_director (tconst, nconst)
            VALUES (%s, %s);
        """

        insert_writer_sql = """
            INSERT INTO title_writer (tconst, nconst)
            VALUES (%s, %s);
        """

        director_batch = []
        writer_batch = []
        director_count = 0
        writer_count = 0

        try:
            conn = connect_db()
            conn.autocommit = False
            cur = conn.cursor()

            for row in rows:
                tconst = row.get("tconst")
                if not tconst:
                    continue

                # FK safety: only keep rows where tconst exists
                if tconst not in existing_title_ids:
                    continue

                # ---- Directors ----
                directors_raw = row.get("directors")
                if directors_raw and directors_raw != r"\N":
                    # dedupe nconsts for this title within the chunk
                    seen_directors = set()
                    for part in directors_raw.split(","):
                        n = part.strip()
                        if not n or n == r"\N":
                            continue
                        if n not in existing_name_ids:
                            continue
                        if n in seen_directors:
                            continue
                        seen_directors.add(n)
                        director_batch.append((tconst, n))
                        director_count += 1

                        if len(director_batch) >= BATCH_SIZE:
                            cur.executemany(insert_director_sql, director_batch)
                            director_batch.clear()
                            conn.commit()

                # ---- Writers ----
                writers_raw = row.get("writers")
                if writers_raw and writers_raw != r"\N":
                    seen_writers = set()
                    for part in writers_raw.split(","):
                        n = part.strip()
                        if not n or n == r"\N":
                            continue
                        if n not in existing_name_ids:
                            continue
                        if n in seen_writers:
                            continue
                        seen_writers.add(n)
                        writer_batch.append((tconst, n))
                        writer_count += 1

                        if len(writer_batch) >= BATCH_SIZE:
                            cur.executemany(insert_writer_sql, writer_batch)
                            writer_batch.clear()
                            conn.commit()

            # Final flush for this chunk
            if director_batch:
                cur.executemany(insert_director_sql, director_batch)
                director_batch.clear()
                conn.commit()

            if writer_batch:
                cur.executemany(insert_writer_sql, writer_batch)
                writer_batch.clear()
                conn.commit()

        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

        return director_count, writer_count

    # -------- Main body: read TSV, build chunks, dispatch to threads --------
    total_directors = 0
    total_writers = 0
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
                print(f"Queued {i} rows from title.crew.tsv")

        # Submit remaining rows
        if current_chunk:
            futures.append(executor.submit(process_chunk, current_chunk))

        for fut in as_completed(futures):
            d_count, w_count = fut.result()
            total_directors += d_count
            total_writers += w_count

    print("Finished loading title_director and title_writer via threads.")
    print(f"Total title_director rows inserted: {total_directors}")
    print(f"Total title_writer rows inserted: {total_writers}")


def main():
    if not TITLE_CREW_TSV.exists():
        raise FileNotFoundError(f"TSV file not found: {TITLE_CREW_TSV}")

    print("Loading existing title IDs from 'title_basics'...")
    existing_title_ids = load_existing_title_ids()
    print(f"Loaded {len(existing_title_ids)} title IDs")

    print("Loading existing name IDs from 'name_basics'...")
    existing_name_ids = load_existing_name_ids()
    print(f"Loaded {len(existing_name_ids)} name IDs")

    print("Multi-threaded load of title.crew.tsv -> title_director/title_writer...")
    load_title_crew_mt(
        TITLE_CREW_TSV,
        existing_title_ids,
        existing_name_ids
    )

    print("All done for title.crew.tsv")


if __name__ == "__main__":
    main()
