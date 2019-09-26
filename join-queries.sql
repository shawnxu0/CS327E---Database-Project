-- Returns movie titles and characters if the film is adult
SELECT primaryTitle, originalTitle, characters 
FROM `coherent-server-252621.imdb_staging.title_basics` t JOIN `coherent-server-252621.imdb_staging.title_principals` c
ON t.tconst = c.tconst AND isAdult = 1
--displays movie titles and directorsID for years after 1970
SELECT primaryTitle, originalTitle, directors, startYear FROM `coherent-server-252621.imdb_staging.title_basics` t JOIN `coherent-server-252621.imdb_staging.title_crew` c
ON t.tconst = c.tconst AND startYear > 1970

-- Returns movie titles, region, and language 
SELECT primaryTitle, originalTitle, region, language 
FROM `coherent-server-252621.imdb_staging.title_basics` t LEFT JOIN `coherent-server-252621.imdb_staging.title_akas` c
ON t.tconst = c.titleId

--Returns people, including directors, ordered by year of birth
SELECT primaryName, birthYear
FROM `coherent-server-252621.imdb_staging.name_basics_fix` LEFT JOIN `coherent-server-252621.imdb_staging.title_crew` on nconst = directors
ORDER BY birthYear

--Returns all titles including titles that have been given ratings, in alphabetical order
SELECT title, region
FROM `coherent-server-252621.imdb_staging.title_akas` LEFT JOIN `coherent-server-252621.imdb_staging.title_ratings` on titleID = tconst
ORDER BY title

--Returns the names of all people who were principal cast members on their titles, ordered by birth year
SELECT primaryName, birthYear
FROM `coherent-server-252621.imdb_staging.name_basics_fix` as name JOIN `coherent-server-252621.imdb_staging.title_principals` as principal on name.nconst = principal.nconst
ORDER BY birthYear