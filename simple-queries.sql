--show episode and season numbers for shows that went over 30 seasons
SELECT episodeNumber, seasonNumber FROM `coherent-server-252621.imdb_staging.title_episode` where seasonNumber > 30 order by seasonNumber

--show all principal jobs that fall under archive_sound ordered by ordering column
SELECT * FROM `coherent-server-252621.imdb_staging.title_principals` where category = "archive_sound" order by ordering

--show entries that scored above a 5.0, ordered by how many people voted for them
SELECT * FROM `coherent-server-252621.imdb_staging.title_ratings` where averageRating > 5.0 order by numVotes

--Show all adult movies/shorts/tv shows, ordered by year of release
SELECT * FROM `coherent-server-252621.imdb_staging.title_basics` where isAdult = 1 order by startYear

--show the titleID of all english language films/tv shows in alphabetical order
SELECT titleID FROM `coherent-server-252621.imdb_staging.title_akas` where language = 'en' order by title

--Show people born before 1900, and order by the year of their death
SELECT * FROM `coherent-server-252621.imdb_staging.name_basics_fix` where birthYear < 1900 order by deathYear

--Select movies/tv shows/videos where the director is also the writer, order by titleID
SELECT * FROM `coherent-server-252621.imdb_staging.title_crew` where directors = writers order by tconst