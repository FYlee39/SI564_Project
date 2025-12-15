USE imdb_normalized;

# 1.
SELECT
    tt.title_type_name,
    COUNT(1) AS number
FROM title_basics AS tb
INNER JOIN title_type AS tt
    ON tb.title_type_id = tt.title_type_id
GROUP BY tt.title_type_id;

# 2.
SELECT
    tb.primaryTitle AS title,
    tr.averageRating AS rating,
    tr.numVotes AS votes
FROM title_basics AS tb
INNER JOIN title_ratings AS tr
    ON tb.tconst = tr.tconst
WHERE tr.numVotes > 1000 ORDER BY tr.averageRating DESC LIMIT 5;

# 3.
SELECT
    tbtt.title_type_name,
    AVG(tr.averageRating) AS average_rating
FROM title_ratings AS tr
INNER JOIN (
    SELECT
        tb.tconst,
        tt.title_type_name
    FROM title_basics AS tb
    INNER JOIN title_type AS tt
        ON tb.title_type_id = tt.title_type_id
    ) AS tbtt
    ON tr.tconst = tbtt.tconst
GROUP BY tbtt.title_type_name;

# 4.
SELECT
    nbtd.primaryName AS name,
    AVG(tr.averageRating) AS average_ratings,
    COUNT(1) AS number_title
FROM title_basics AS tb
INNER JOIN (
    SELECT
        nb.primaryName,
        nb.nconst,
        td.tconst
    FROM name_basics AS nb
    INNER JOIN title_director AS td
        ON nb.nconst = td.nconst
    ) AS nbtd
    ON tb.tconst = nbtd.tconst
INNER JOIN title_ratings AS tr
    ON tb.tconst = tr.tconst
GROUP BY nbtd.nconst HAVING number_title >= 5 ORDER BY average_ratings DESC LIMIT 5;

# 5.
SELECT
    COUNT(1) AS number
FROM name_basics AS nb
WHERE nb.nconst IN (
    SELECT
        td.nconst
    FROM title_director AS td
    )
AND nb.nconst IN (
    SELECT
        td.nconst
    FROM title_writer AS td);

# 6.
SELECT
    p.profession_name AS profession_name,
    COUNT(DISTINCT pp.nconst) AS number
FROM profession AS p
INNER JOIN person_profession AS pp
    ON p.id = pp.profession_id
GROUP BY p.profession_name;

# 7.
CREATE INDEX idx_title_genre_genre_tconst
    ON title_genre (genre_id, tconst);

CREATE INDEX idx_title_director_tconst_nconst
    ON title_director (tconst, nconst);

CREATE INDEX idx_title_director_nconst_tconst
    ON title_director (nconst, tconst);

SELECT
    dc.genre_name AS genre,
    dc.director_name AS name,
    dc.num_titles_directed AS num_works
FROM (
    SELECT
        g.genre_id,
        g.genre_name,
        nb.nconst,
        nb.primaryName AS director_name,
        COUNT(1) AS num_titles_directed
    FROM genre AS g
    INNER JOIN title_genre AS tg
        ON g.genre_id = tg.genre_id
    INNER JOIN title_director AS td
        ON tg.tconst = td.tconst
    INNER JOIN name_basics AS nb
        ON td.nconst = nb.nconst
    GROUP BY
        g.genre_id,
        g.genre_name,
        nb.nconst,
        nb.primaryName
) AS dc
INNER JOIN (
    SELECT
        g.genre_id,
        MAX(cnt) AS max_cnt
    FROM (
        SELECT
            g.genre_id,
            nb.nconst,
            COUNT(1) AS cnt
        FROM genre AS g
        JOIN title_genre AS tg
            ON g.genre_id = tg.genre_id
        JOIN title_director AS td
            ON tg.tconst = td.tconst
        JOIN name_basics AS nb
            ON td.nconst = nb.nconst
        GROUP BY g.genre_id, nb.nconst
    ) AS inner_counts
    INNER JOIN genre AS g
        ON inner_counts.genre_id = g.genre_id
    GROUP BY g.genre_id
) AS mc
    ON dc.genre_id = mc.genre_id
   AND dc.num_titles_directed = mc.max_cnt
ORDER BY
    dc.genre_name;