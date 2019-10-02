-- Returns movie titles and characters if the film is adult
SELECT primaryTitle, originalTitle, characters 
FROM `coherent-server-252621.imdb_modeled.title_basics` t JOIN `coherent-server-252621.imdb_modeled.Principles` c
ON t.tconst = c.tconst AND isAdult = 1

--displays movie titles and directorsID for years after 1970
SELECT primaryTitle, originalTitle, directors, startYear FROM `coherent-server-252621.imdb_modeled.title_basics` t JOIN `coherent-server-252621.imdb_modeled.Crew` c
ON t.tconst = c.tconst AND startYear > 1970

-- Returns movie titles, region, and language for episodes
SELECT primaryTitle, originalTitle, region, language, episodeNumber
FROM `coherent-server-252621.imdb_modeled.title_basics` t RIGHT JOIN `coherent-server-252621.imdb_modeled.Episodes` c
ON t.tconst = c.tconst AND c.episodeNumber is not null

--Returns people, including directors, ordered by year of birth
SELECT distinct primaryName, birthYear
FROM imdb_modeled.People LEFT JOIN imdb_modeled.Crew on nconst = directors
ORDER BY birthYear

--Returns all titles including titles that have been given ratings, in alphabetical order
SELECT distinct primaryTitle, region
FROM imdb_modeled.title_basics as tb LEFT JOIN imdb_modeled.Ratings as r on tb.tconst = r.tconst
ORDER BY primaryTitle

--Returns the names of all people who were principal cast members on their titles, ordered by birth year
SELECT distinct primaryName, birthYear
FROM imdb_modeled.People as name JOIN imdb_modeled.Principles as principal on name.nconst = principal.nconst
ORDER BY birthYear
