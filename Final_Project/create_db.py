import mysql.connector as mysql

# ---- UPDATE THESE AS NEEDED ----
DB_HOST = "*"
DB_PORT = 3306
DB_USER = "*"
DB_PASS = "*"
DB_NAME = "*"
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
            startYear INT NULL,
            endYear INT NULL,
            runtimeMinutes INT NULL,
            title_type_id INT NULL,
            CONSTRAINT fk_tt_type_id
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
        CONSTRAINT fk_tg_title
            FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
            ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_tg_genere_id
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
        birthYear INT NULL,
        deathYear INT NULL
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
        position INT NOT NULL,
        CONSTRAINT fk_nt_nconst
            FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
            ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_nt_tconst
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
        CONSTRAINT fk_tt_titleId
            FOREIGN KEY (titleId) REFERENCES title_basics(tconst) 
        ON UPDATE CASCADE ON DELETE CASCADE
    );
    """,

    # Lookup: types (e.g., "tv", "festival", "dvd", etc.)
    """
    CREATE TABLE IF NOT EXISTS types (
        id INT AUTO_INCREMENT PRIMARY KEY,
        type_name VARCHAR(512) NOT NULL
    );
    """,

    # Bridge: title_aka_type
    """
    CREATE TABLE IF NOT EXISTS title_aka_type (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title_akas_id INT NOT NULL,
        title_types_id INT NOT NULL,
        CONSTRAINT fk_tt_title_akas_id
            FOREIGN KEY (title_akas_id) REFERENCES title_akas(id) 
        ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_tt_title_types_id
            FOREIGN KEY (title_types_id) REFERENCES types(id) 
        ON UPDATE CASCADE ON DELETE CASCADE
    );
    """,

    # Lookup: title_attribute (e.g., "working title", "short title", etc.)
    """
    CREATE TABLE IF NOT EXISTS title_attribute (
        id INT AUTO_INCREMENT PRIMARY KEY,
        attribute_name VARCHAR(128) NOT NULL
    );
    """,

    # Bridge: title_aka_attribute
    """
    CREATE TABLE IF NOT EXISTS title_aka_attribute (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title_akas_id INT NOT NULL,
        title_attribute_id INT NOT NULL,
        CONSTRAINT fk_ta_title_akas_id
            FOREIGN KEY (title_akas_id) REFERENCES title_akas(id) 
        ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_ta_title_types_id
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
        CONSTRAINT fk_tp_tconst
            FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
            ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_tp_nconst
            FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
            ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_tp_category_id
            FOREIGN KEY (category_id) REFERENCES principal_category(id)
            ON UPDATE CASCADE ON DELETE CASCADE
    );
    """,

    # Bridge table: principal_character
    """
    CREATE TABLE IF NOT EXISTS principal_character (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title_principals_id INT NOT NULL,
        character_name VARCHAR(512) NULL,
        CONSTRAINT fk_tp_title_principals_id
            FOREIGN KEY (title_principals_id) REFERENCES title_principals(id)
            ON UPDATE CASCADE ON DELETE CASCADE
    );
    """
]

# Normalized title_crew
normalized_title_crew = [
    # Bridge: title_director
    """
    CREATE TABLE IF NOT EXISTS title_director (
        id INT AUTO_INCREMENT PRIMARY KEY,
        tconst VARCHAR(12) NOT NULL,
        nconst VARCHAR(12) NOT NULL,
        CONSTRAINT fk_td_tconst
            FOREIGN KEY (tconst)
            REFERENCES title_basics(tconst)
            ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_td_nconst
            FOREIGN KEY (nconst)
            REFERENCES name_basics(nconst)
            ON UPDATE CASCADE ON DELETE CASCADE
    );
    """,

    # Bridge: title_writer
    """
    CREATE TABLE IF NOT EXISTS title_writer (
        id INT AUTO_INCREMENT PRIMARY KEY,
        tconst VARCHAR(12) NOT NULL,
        nconst VARCHAR(12) NOT NULL,
        CONSTRAINT fk_tw_tconst
            FOREIGN KEY (tconst)
            REFERENCES title_basics(tconst)
            ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_tw_nconst
            FOREIGN KEY (nconst)
            REFERENCES name_basics(nconst)
            ON UPDATE CASCADE ON DELETE CASCADE
    );
    """
]

# Rest of tables
normalized_title_episode_AND_title_ratings = [
    # Table: title_episode
    """
    CREATE TABLE IF NOT EXISTS title_episode (
        tconst VARCHAR(12) PRIMARY KEY NOT NULL,
        parentTconst VARCHAR(12) NULL,
        seasonNumber INT NULL,
        episodeNumber INT NULL,
        CONSTRAINT fk_te_tconst
            FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
            ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_te_parentTconst
            FOREIGN KEY (parentTconst) REFERENCES title_basics(tconst)
            ON UPDATE CASCADE ON DELETE CASCADE
    );
    """,

    # Table: title_ratings
    """
    CREATE TABLE IF NOT EXISTS title_ratings (
        tconst VARCHAR(12) PRIMARY KEY NOT NULL,
        averageRating DECIMAL(3,1) NULL,
        numVotes INT NULL,
        CONSTRAINT fk_tr_tconst
            FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
            ON UPDATE CASCADE ON DELETE CASCADE
    );
    """
]


DBC_STATEMENTS = [
    start_Statements,
    normalized_title_basic,
    normalized_name_basic,
    normalized_title_akas,
    normalized_title_principals,
    normalized_title_crew,
    normalized_title_episode_AND_title_ratings
]

def main():
    conn = mysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS
    )

    cur = conn.cursor()

    for statement_list in DBC_STATEMENTS:
        for query in statement_list:
            try:
                cur.execute(query)
            except mysql.Error as err:
                print(f"Error executing:\n{query}\n→ {err}")
                raise

    cur.close()
    conn.close()

    print("Normalized title_basics schema created successfully.")


if __name__ == "__main__":
    main()
