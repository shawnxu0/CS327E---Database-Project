--Creates a new table from title_episode
create table imdb_modeled.Episodes as 
select *
from imdb_staging.title_episode

--Creates a new table from title_crew
create table imdb_modeled.Crew as 
select *
from imdb_staging.title_crew

--Creates a new table using values from name_basics_fix
create table imdb_modeled.People as
select distinct nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles
from imdb_staging.name_basics_fix
where nconst is not null
order by nconst

--Creates merged table in imdb_modeled between title_basics and title_akas from imdb_staging
CREATE table imdb_modeled.title_basics as 
SELECT tconst, titleType, primaryTitle, originalTitle, title, startYear, cast(endYear as int64) as endYear, runtimeMinutes, genres, ordering, region, language, types, attributes, isAdult, isOriginalTitle
FROM imdb_staging.title_basics
FULL OUTER JOIN imdb_staging.title_akas
ON titleID = tconst

--Creates table in imdb_modeled for Ratings
CREATE TABLE imdb_modeled.Ratings as
select *
from imdb_staging.title_ratings

--Creates table in imdb_modeled for Principles
CREATE TABLE imdb_modeled.Principles as
select *
from imdb_staging.title_principals