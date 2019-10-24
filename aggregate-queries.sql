--show the number of titles made per year after 1950
SELECT startYear, COUNT(tconst) as num_titles
FROM `coherent-server-252621.imdb_modeled.title_basics`
GROUP BY startYear
HAVING startYear > 1950
ORDER BY startYear

--show the number of titles that belong to each primary genre (genre 1), excluding titles without a primary genre 
SELECT genre1 as Primary_Genre, COUNT(tconst) as num_titles
FROM `coherent-server-252621.imdb_modeled.Genres_Beam_DF`
GROUP BY genre1
HAVING genre1 is not null

--show the average rating of titles, grouped by the number of votes that were received
SELECT numVotes as received_votes, AVG(averageRating) as average_rating
FROM `coherent-server-252621.imdb_modeled.Ratings`
GROUP BY numVotes
HAVING numVotes > 0
ORDER BY numVotes desc

--returns the total number of professions for each first profession across all entries
select profession1, count(profession1) as num_professions_total
from imdb_modeled.primaryProfessions_Beam_DF
group by profession1
order by profession1

--returns the number of entries in each region
select region, count(region) as num_entries_in_region
from imdb_modeled.title_basics
group by region
order by region

--returns movies having US as a region and long runtimes
select titleType, title, region, max(runtimeMinutes) as longest_runtimes
from imdb_modeled.title_basics
group by title, titleType, region
having region ="US"
and longest_runtimes is not null
order by titleType