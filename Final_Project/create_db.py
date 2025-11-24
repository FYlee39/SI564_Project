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
        title_type_name VARCHAR(64) NOT NULL
    );
    """,

    # Core table: title_basics
    # Foreign Key: title_type_id
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
            CONSTRAINT fk_title_type_id
                FOREIGN KEY (title_type_id) REFERENCES title_type(title_type_id)
            ON UPDATE CASCADE ON DELETE CASCADE
        );
        """,

    # Lookup: genre
    # Split genres by comma, distinct genre rows.
    """
    CREATE TABLE IF NOT EXISTS genre (
        genre_id INT AUTO_INCREMENT PRIMARY KEY,
        genre_name VARCHAR(64) NOT NULL
    );
    """,

    # Bridge table: title_genre (many-to-many)
    # Foreign keys: tconst, genre_id
    """
    CREATE TABLE IF NOT EXISTS title_genre (
        id INT AUTO_INCREMENT PRIMARY KEY,
        tconst VARCHAR(12) NOT NULL,
        genre_id INT NOT NULL,
        CONSTRAINT fk_tconst
            FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
            ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_genere_id
            FOREIGN KEY (genre_id) REFERENCES genre(genre_id)
            ON UPDATE CASCADE ON DELETE CASCADE
    );
    """
]

# Normalized "name basic"
normalized_name_basic = [
    # Core table: name_basics
    """
    CREATE TABLE IF NOT EXISTS name_basics (
        nconst VARCHAR(12) NOT NULL PRIMARY KEY,
        primaryName VARCHAR(256) NULL,
        birthYear SMALLINT NULL,
        deathYear SMALLINT NULL,
    );
    """,

    # Lookup: profession
    # Split primaryProfession.
    """
    CREATE TABLE IF NOT EXISTS profession (
        id INT AUTO_INCREMENT PRIMARY KEY,
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
    # Foreign Keys: nconst, profession_id
    """
    CREATE TABLE IF NOT EXISTS name_known_for (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nconst VARCHAR(12) NOT NULL,
        tconst VARCHAR(12) NOT NULL,
        position SMALLINT NOT NULL,
        CONSTRAINT fk_nconst
            FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
            ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_tconst
            FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
            ON UPDATE CASCADE ON DELETE CASCADE
    );
    """
]

# Normalized title_akas
normalized_title_akas = [
    # Core table: title_akas
    # Foreign Key: titleId
    """
    CREATE TABLE IF NOT EXISTS title_akas (
        id INT AUTO_INCREMENT PRIMARY KEY,
        titleId VARCHAR(12) NOT NULL,
        ordering INT NOT NULL,
        title VARCHAR(512) NULL,
        region_code VARCHAR(16) NULL,
        language_code VARCHAR(16) NULL,
        isOriginalTitle TINYINT(1) NULL,
        CONSTRAINT fk_titleId
            FOREIGN KEY (titleId) REFERENCES title_basics(tconst) 
        ON UPDATE CASCADE ON DELETE CASCADE
    );
    """,

    # Lookup: title_type (e.g., "tv", "festival", "dvd", etc.)
    """
    CREATE TABLE IF NOT EXISTS title_types (
        id INT AUTO_INCREMENT PRIMARY KEY,
        type_name VARCHAR(512) NOT NULL,
    );
    """,

    # Bridge: title_aka_type
    """
    CREATE TABLE IF NOT EXISTS title_aka_type (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title_akas_id INT NOT NULL,
        title_types_id INT NOT NULL,
        CONSTRAINT fk_title_akas_id
            FOREIGN KEY (title_akas_id) REFERENCES title_akas(id) 
        ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_title_types_id
            FOREIGN KEY (title_types_id) REFERENCES title_types(id) 
        ON UPDATE CASCADE ON DELETE CASCADE
    );
    """,

    # Lookup: title_attribute (e.g., "working title", "short title", etc.)
    """
    CREATE TABLE IF NOT EXISTS title_attribute (
        id INT AUTO_INCREMENT PRIMARY KEY,
        attribute_name VARCHAR(128) NOT NULL,
    );
    """,
    
    # Bridge: title_aka_attribute
    """
    CREATE TABLE IF NOT EXISTS title_aka_attribute (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title_akas_id INT NOT NULL,
        title_attribute_id INT NOT NULL,
        CONSTRAINT fk_title_akas_id
            FOREIGN KEY (title_akas_id) REFERENCES title_akas(id) 
        ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_title_types_id
            FOREIGN KEY (title_attribute_id) REFERENCES title_attribute(id) 
        ON UPDATE CASCADE ON DELETE CASCADE
    );
    """,
]

# Normalized title principals
normalized_title_principals = [
    # Lookup: principal_category (e.g., "actor", "actress", "director", "producer", ...)
    """
    CREATE TABLE IF NOT EXISTS principal_category (
        id INT AUTO_INCREMENT PRIMARY KEY,
        category_name VARCHAR(64) NOT NULL
    );
    """,

    # Core table: title_principals
    """
    CREATE TABLE IF NOT EXISTS title_principals (
        id INT AUTO_INCREMENT PRIMARY KEY,
        tconst VARCHAR(12) NOT NULL,
        ordering INT NOT NULL,
        nconst VARCHAR(12)  NOT NULL,
        category_id  INT NULL,
        job VARCHAR(256) NULL,
        CONSTRAINT fk_tconst
            FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        CONSTRAINT fk_nconst
            FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        CONSTRAINT fk_category_id
            FOREIGN KEY (category_id) REFERENCES principal_category(id)
            ON UPDATE CASCADE
            ON DELETE SET NULL
    );
    """,

    # Bridge table: principal_character
    """
    CREATE TABLE IF NOT EXISTS principal_character (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title_principals_id INT NOT NULL,
        character VARCHAR(512) NULL,
        CONSTRAINT fk_title_principals_id
            FOREIGN KEY title_principals_id
            REFERENCES title_principals(id)
            ON UPDATE CASCADE
            ON DELETE CASCADE
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
