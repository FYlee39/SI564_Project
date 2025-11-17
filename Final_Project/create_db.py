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
    """
    CREATE TABLE IF NOT EXISTS title_type (
        title_type_id INT AUTO_INCREMENT PRIMARY KEY,
        title_type_name VARCHAR(64),
    );
    """,

    # Lookup: genre
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
