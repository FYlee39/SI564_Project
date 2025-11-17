import mysql.connector as mysql

# ---- UPDATE THESE AS NEEDED ----
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "root"
DB_PASS = "your_password"
DB_NAME = "imdb_normalized"
# --------------------------------

# Create databases if not exist
start_Statements = [
    f"CREATE DATABASE IF NOT EXISTS {DB_NAME};",
    f"USE {DB_NAME};",
]

# Normalized "title_basic"
normalized_title_basic = [
    # Lookup: title_type
    # Split title type by comma, distinct types rows.
    """
    CREATE TABLE IF NOT EXISTS title_type (
        title_type_id INT AUTO_INCREMENT PRIMARY KEY,
        title_type_name VARCHAR(64),
    );
    """,

    # Lookup: genre
    # Split genres by comma, distinct genre rows.
    """
    CREATE TABLE IF NOT EXISTS genre (
        genre_id INT AUTO_INCREMENT PRIMARY KEY,
        genre_name VARCHAR(64)
    );
    """,

    # Core table: title_basics (normalized)
    """
    CREATE TABLE IF NOT EXISTS title_basics (
        tconst VARCHAR(12) NOT NULL PRIMARY KEY,
        primaryTitle VARCHAR(512) NULL,
        originalTitle VARCHAR(512) NULL,
        isAdult TINYINT(1) NULL,
        startYear SMALLINT NULL,
        endYear SMALLINT NULL,
        runtimeMinutes SMALLINT NULL,
        title_type_id INT NULL,
    );
    """,

    # Bridge table: title_genre (many-to-many)
    """
    CREATE TABLE IF NOT EXISTS title_genre (
        id INT AUTO_INCREMENT PRIMARY KEY,
        tconst VARCHAR(12) NOT NULL,
        genre_id INT NOT NULL,
    );
    """
]

normalized_name_basic = [

    # Core table: name_basics (normalized, no multivalue fields)
    """
    CREATE TABLE IF NOT EXISTS name_basics (
        nconst VARCHAR(12) NOT NULL PRIMARY KEY,
        primaryName VARCHAR(256) NULL,
        birthYear SMALLINT NULL,
        deathYear SMALLINT NULL
    );
    """,

    # Lookup: profession
    # Split primaryProfession.
    """
    CREATE TABLE IF NOT EXISTS profession (
        profession_id INT AUTO_INCREMENT PRIMARY KEY,
        profession_name VARCHAR(128) NOT NULL
    );
    """,

    # Bridge: person_profession (name_basics : profession)
    """
    CREATE TABLE IF NOT EXISTS person_profession (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nconst VARCHAR(12) NOT NULL,
        profession_id INT NOT NULL
    );
    """,

    # Bridge: name_known_for (name_basics : title_basics with ordering)
    # Split knownForTitles
    """
    CREATE TABLE IF NOT EXISTS name_known_for (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nconst VARCHAR(12) NOT NULL,
        tconst VARCHAR(12) NOT NULL,
        position SMALLINT NOT NULL,   -- order from the original list
    );
    """
]

DDL_STATEMENTS = [
    normalized_title_basic,
                  ]

def main():
    conn = mysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS
    )

    cur = conn.cursor()

    for stmt in DDL_STATEMENTS:
        for sql in [s.strip() + ";" for s in stmt.split(";") if s.strip()]:
            try:
                cur.execute(sql)
            except mysql.Error as err:
                print(f"Error executing:\n{sql}\n→ {err}")
                raise

    cur.close()
    conn.close()

    print("Normalized title_basics schema created successfully.")


if __name__ == "__main__":
    main()
