SQL Queries:

To find primary keys in each table:
SELECT COUNT (DISTINCT nconst) FROM `coherent-server-252621.imdb_staging.name_basics_fix`

SELECT COUNT (DISTINCT titleID) FROM `coherent-server-252621.imdb_staging.title_akas`

SELECT COUNT (DISTINCT tconst) FROM `coherent-server-252621.imdb_staging.title_basics`

SELECT COUNT (DISTINCT tconst) FROM `coherent-server-252621.imdb_staging.title_crew`

SELECT COUNT (DISTINCT tconst) FROM `coherent-server-252621.imdb_staging.title_principals`

SELECT COUNT (DISTINCT tconst) FROM `coherent-server-252621.imdb_staging.title_ratings`

