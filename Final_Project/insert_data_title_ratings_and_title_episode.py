from connect_db import *
import csv
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed


# ---------------- CONFIG ----------------

TITLE_EPISODE_TSV = Path("C:\\My_Programs\\Temp\\Data\\title.episode.tsv")
TITLE_RATINGS_TSV = Path("C:\\My_Programs\\Temp\\Data\\title.ratings.tsv")

BATCH_SIZE = 200    # per-thread batch size for executemany

# ----------------------------------------


def parse_int(value):
    # Return int
    if value is None or value == "" or value == r"\N":
        return None
    try:
        return int(value)
    except ValueError:
        return None

def parse_float(value):
    # Return float
    if value is None or value == "" or value == r"\N":
        return None
    try:
        return(float(value))
    except ValueError:
        return None


# ---------------- STEP 1: get existing title IDs for FK safety ----------------

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

# =========================
# 1) title.episode.tsv
# =========================

def load_title_episode_mt(
    tsv_path: Path,
    existing_title_ids,
    max_workers: int=5,
    chunk_size: int=1000,
):
    """
    Multi-threaded loader for title.episode.tsv

    Raw columns:
      - tconst        (episode id)
      - parentTconst  (series id)
      - seasonNumber
      - episodeNumber

    Target table:
      - title_episode(tconst PK, parentTconst, seasonNumber, episodeNumber)
      - FKs to title_basics(tconst) and title_basics(parentTconst)
    """

    def process_chunk(rows):
        """
        Process a list of CSV rows in a single thread:
        - Build batch for title_episode
        - Insert in bulk
        """
        conn = connect_db()
        conn.autocommit = False
        cur = conn.cursor()

        insert_episode_sql = """
            INSERT INTO title_episode (
                tconst,
                parentTconst,
                seasonNumber,
                episodeNumber
            ) VALUES (%s, %s, %s, %s);
        """

        episode_batch = []
        episode_count = 0

        try:
            for row in rows:
                tconst = row.get("tconst")
                if not tconst:
                    continue

                # FK safety: ensure the episode title exists
                if tconst not in existing_title_ids:
                    continue

                parentTconst = row.get("parentTconst")
                if parentTconst in (None, r"\N"):
                    parentTconst = None
                else:
                    # only keep if parent also in title_basics
                    if parentTconst not in existing_title_ids:
                        # set None if not
                        parentTconst = None

                seasonNumber = parse_int(row.get("seasonNumber"))
                episodeNumber = parse_int(row.get("episodeNumber"))

                episode_batch.append(
                    (tconst, parentTconst, seasonNumber, episodeNumber)
                )
                episode_count += 1

                if len(episode_batch) >= BATCH_SIZE:
                    cur.executemany(insert_episode_sql, episode_batch)
                    episode_batch.clear()
                    conn.commit()

            # Final flush for this chunk
            if episode_batch:
                cur.executemany(insert_episode_sql, episode_batch)
                episode_batch.clear()
                conn.commit()

        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

        return episode_count

    # ---- main body: read TSV, chunk, dispatch to threads ----
    total_episodes = 0
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
                print(f"Queued {i} rows from title.episode.tsv")

        # Submit remaining rows
        if current_chunk:
            futures.append(executor.submit(process_chunk, current_chunk))

        for fut in as_completed(futures):
            ep_count = fut.result()
            total_episodes += ep_count

    print("Finished loading title_episode via threads.")
    print(f"Total title_episode rows inserted: {total_episodes}")


# =========================
# 2) title.ratings.tsv
# =========================

def load_title_ratings_mt(
    tsv_path: Path,
    existing_title_ids,
    max_workers: int=5,
    chunk_size: int=1000,
):
    """
    Multi-threaded loader for title.ratings.tsv

    Raw columns:
      - tconst
      - averageRating
      - numVotes

    Target table:
      - title_ratings(tconst PK, averageRating, numVotes)
      - FK(tconst -> title_basics.tconst)
    """

    def process_chunk(rows):
        """
        Process a list of CSV rows in a single thread:
        - Build batch for title_ratings
        - Insert in bulk
        """
        conn = connect_db()
        conn.autocommit = False
        cur = conn.cursor()

        insert_ratings_sql = """
            INSERT INTO title_ratings (
                tconst,
                averageRating,
                numVotes
            ) VALUES (%s, %s, %s);
        """

        ratings_batch = []
        ratings_count = 0

        try:
            for row in rows:
                tconst = row.get("tconst")
                if not tconst:
                    continue

                # FK safety: only keep ratings for titles we have
                if tconst not in existing_title_ids:
                    continue

                avg_raw = row.get("averageRating")
                num_raw = row.get("numVotes")

                averageRating = parse_float(avg_raw)
                numVotes = parse_int(num_raw)

                ratings_batch.append((tconst, averageRating, numVotes))
                ratings_count += 1

                if len(ratings_batch) >= BATCH_SIZE:
                    cur.executemany(insert_ratings_sql, ratings_batch)
                    ratings_batch.clear()
                    conn.commit()

            # Final flush for this chunk
            if ratings_batch:
                cur.executemany(insert_ratings_sql, ratings_batch)
                ratings_batch.clear()
                conn.commit()

        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

        return ratings_count

    # ---- main body: read TSV, chunk, dispatch to threads ----
    total_ratings = 0
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
                print(f"Queued {i} rows from title.ratings.tsv")

        # Submit remaining rows
        if current_chunk:
            futures.append(executor.submit(process_chunk, current_chunk))

        for fut in as_completed(futures):
            r_count = fut.result()
            total_ratings += r_count

    print("Finished loading title_ratings via threads.")
    print(f"Total title_ratings rows inserted: {total_ratings}")


# =========================
# MAIN
# =========================

def main():
    print("Loading existing title IDs from 'title_basics'...")
    existing_title_ids = load_existing_title_ids()
    print(f"Loaded {len(existing_title_ids)} title IDs")

    if TITLE_EPISODE_TSV.exists():
        print("Multi-threaded load of title.episode.tsv -> title_episode...")
        load_title_episode_mt(
            TITLE_EPISODE_TSV,
            existing_title_ids
        )
    else:
        print(f"WARNING: {TITLE_EPISODE_TSV} not found; skipping title_episode load.")

    if TITLE_RATINGS_TSV.exists():
        print("Multi-threaded load of title.ratings.tsv -> title_ratings...")
        load_title_ratings_mt(
            TITLE_RATINGS_TSV,
            existing_title_ids
        )
    else:
        print(f"WARNING: {TITLE_RATINGS_TSV} not found; skipping title_ratings load.")

    print("Done with title_episode and title_ratings ETL.")


if __name__ == "__main__":
    main()
