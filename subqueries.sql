--Returns results with values for average rating that are greater than the average
SELECT tconst, averageRating FROM `coherent-server-252621.imdb_modeled.Ratings`
WHERE averageRating > (
SELECT AVG(averageRating) FROM `coherent-server-252621.imdb_modeled.Ratings`)

--Returns movie titles and genres where the main genre is either history or mystery
SELECT t.tconst, t.primaryTitle, g.genre1, g.genre2, g.genre3 FROM `coherent-server-252621.imdb_modeled.title_basics` t
JOIN `coherent-server-252621.imdb_modeled.Genres_Beam_DF` g ON t.tconst = g.tconst
WHERE g.genre1 in (
SELECT genre1 FROM `coherent-server-252621.imdb_modeled.Genres_Beam_DF` WHERE genre1 = 'History' OR genre1 = 'Mystery')

--Returns tconst, and primary title for every title that recieved a perfect score.
SELECT tb.tconst, tb.primaryTitle, averageRating FROM `coherent-server-252621.imdb_modeled.Ratings` as r join `coherent-server-252621.imdb_modeled.title_basics` as tb
on r.tconst = tb.tconst
WHERE averageRating = 
(SELECT MAX(averageRating) FROM `coherent-server-252621.imdb_modeled.Ratings` )

--Returns tconst, primary title, and runtime for titles with a higher than average runtime.
SELECT tconst, primaryTitle, runtimeMinutes FROM `coherent-server-252621.imdb_modeled.title_basics`
WHERE runtimeMinutes > (
SELECT AVG(runtimeMinutes) FROM `coherent-server-252621.imdb_modeled.title_basics`)