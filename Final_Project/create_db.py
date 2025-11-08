# pip install mysql-connector-python
import mysql.connector as mysql

# --- Update these for your MySQL instance ---
DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_USER = "root"
DB_PASS = "password"
DB_NAME = "imdb"
# -------------------------------------------

DDL_STATEMENTS = [
    # Create database and select it
    f"CREATE DATABASE IF NOT EXISTS {DB_NAME}",
    f"USE {DB_NAME};",

    # 1) title_basics

    """
    CREATE TABLE IF NOT EXISTS title_basics (
        tconst VARCHAR(12) NOT NULL,
        titleTyep VARCHAR(512),
        primaryTitle VARCHAR(512),
        originalTitle VARCHAR(512),
        isAdult TINYINT(1),
        startYear SMALLINT,
        endYear SMALLINT,
        runtimeMinutes SMALLINT,
        genres VARCHAR(128),
        CONSTRAINT pk PRIMARY KEY (tconst)
    );
    """

    # 2) name_basics
    """
    CREATE TABLE IF NOT EXISTS name_basics (
        nconst VARCHAR(12) NOT NULL,
        primaryName VARCHAR(256),
        birthYear SMALLINT,
        deathYear SMALLINT,
        primaryProfession VARCHAR(128),   -- comma-separated in raw TSV
        knownForTitles VARCHAR(128),   -- comma-separated tconsts in raw TSV
        CONSTRAINT pk PRIMARY KEY (nconst)
    );
    """,

    # 3) title_akas (composite PK: titleId + ordering)
    """
    CREATE TABLE IF NOT EXISTS title_akas (
        titleId VARCHAR(12) NOT NULL,
        ordering INT NOT NULL,
        title VARCHAR(512),
        region VARCHAR(16),
        language VARCHAR(16),
        types VARCHAR(64),   -- comma-separated values in raw TSV
        attributes VARCHAR(128),   -- comma-separated values in raw TSV
        isOriginalTitle TINYINT(1),
        CONSTRAINT pk PRIMARY KEY (titleId, ordering)
    );
    """,

    # 4) title_principals (bridge between titles and names)
    """
    CREATE TABLE IF NOT EXISTS title_principals (
        tconst VARCHAR(12)  NOT NULL,
        ordering INT NOT NULL,
        nconst VARCHAR(12) NOT NULL,
        category VARCHAR(64),
        job VARCHAR(256),
        characters TEXT,
        CONSTRAINT pk PRIMARY KEY (tconst, ordering)
    );
    """,

    # 5) title_episode (episode -> parent series)
    """
    CREATE TABLE IF NOT EXISTS title_episode (
        tconst        VARCHAR(12) NOT NULL,  -- episode id
        parentTconst  VARCHAR(12) NULL,      -- series id
        seasonNumber  INT         NULL,
        episodeNumber INT         NULL,
        CONSTRAINT pk PRIMARY KEY (tconst)
    );
    """,

    # 6) title_crew (as delivered in TSV: comma-separated IDs; keep as is)
    # If you prefer full normalization, create title_directors/title_writers bridge tables instead.
    """
    CREATE TABLE IF NOT EXISTS title_crew (
        tconst    VARCHAR(12)  NOT NULL,
        directors VARCHAR(1024) NULL,   -- comma-separated nconsts
        writers   VARCHAR(1024) NULL,   -- comma-separated nconsts
        CONSTRAINT pk PRIMARY KEY (tconst)
    );
    """,

    # 7) title_ratings (1–1 with title)
    """
    CREATE TABLE IF NOT EXISTS title_ratings (
        tconst         VARCHAR(12) NOT NULL,
        averageRating  DECIMAL(3,1) NULL,
        numVotes       INT UNSIGNED NULL,
        CONSTRAINT pk PRIMARY KEY (tconst)
    );
    """,

    # --- OPTIONAL: normalized bridge tables for crew (only if you want strict 3NF) ---
    # Uncomment the two tables below AND later transform the CSV lists into rows at load time.
    """
    CREATE TABLE IF NOT EXISTS title_director (
        id INT NOT NULL AUTO_INCREMENT,
        tconst VARCHAR(12) NOT NULL,
        nconst VARCHAR(12) NOT NULL,
        CONSTRAINT pk PRIMARY KEY (id)
    );
    """,


    """
    CREATE TABLE IF NOT EXISTS title_writer (
        id INT NOT NULL AUTO_INCREMENT,
        tconst VARCHAR(12) NOT NULL,
        nconst VARCHAR(12) NOT NULL,
        CONSTRAINT pk PRIMARY KEY (id)
    );
    """
]

def main():
    conn = mysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, autocommit=True
    )
    cur = conn.cursor()
    for stmt in DDL_STATEMENTS:
        for part in [s for s in stmt.split(";") if s.strip()]:
            sql = part.strip() + ";"
            try:
                cur.execute(sql)
                # print(f"Executed: {sql[:80]}...")
            except mysql.Error as e:
                print(f"Error executing statement:\n{sql}\n--> {e}")
                raise
    cur.close()
    conn.close()
    print(f"Database '{DB_NAME}' and tables created successfully.")

if __name__ == "__main__":
    main()
