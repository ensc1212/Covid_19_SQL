-- CHANGE SERVER SETTINGS TO ACCEPT LOAD DATA LOCAL INFILE
-- CHECK IF IT IS ON OR OFF USING THIS:
SHOW VARIABLES LIKE 'local_infile';
-- SET IT ON WITH THIS:
SET GLOBAL local_infile = 1;
-- SET CLIENT SIDE BY GOING TO HOME, RIGHT CLICK CONNECTION, EDIT CONNECTION, ADVANCED, OTHERS:
-- KEY IN: OPT_LOCAL_INFILE=1

USE Portfolio_Project;

CREATE TABLE covid_vaccinations(
id INT NOT NULL,
iso_code TEXT NULL,
continent TEXT NULL,
location TEXT NULL,
date TEXT NULL,
new_tests DOUBLE NULL,
total_tests DOUBLE NULL,
total_tests_per_thousand DOUBLE NULL,
new_tests_per_thousand DOUBLE NULL,
new_tests_smoothed DOUBLE NULL,
new_tests_smoothed_per_thousand DOUBLE NULL, 
positive_rate DOUBLE NULL,
tests_per_case DOUBLE NULL,
tests_units TEXT NULL,
total_vaccinations DOUBLE NULL,
people_vaccinated DOUBLE NULL,
people_fully_vaccinated DOUBLE NULL,
new_vaccinations DOUBLE NULL,
new_vaccinations_smoothed DOUBLE NULL,
total_vaccinations_per_hundred DOUBLE NULL,
people_vaccinated_per_hundred DOUBLE NULL,
people_fully_vaccinated_per_hundred DOUBLE NULL,
new_vaccinations_smoothed_per_million DOUBLE NULL,
stringency_index DOUBLE NULL,
population_density DOUBLE NULL,
median_age DOUBLE NULL,
aged_65_older DOUBLE NULL,
aged_70_older DOUBLE NULL,
gdp_per_capita DOUBLE NULL,
extreme_poverty DOUBLE NULL,
cardiovasc_death_rate DOUBLE NULL,
diabetes_prevalence DOUBLE NULL,
female_smokers DOUBLE NULL,
male_smokers DOUBLE NULL,
handwashing_facilities DOUBLE NULL,
hospital_beds_per_thousand DOUBLE NULL,
life_expectancy DOUBLE NULL,
human_development_index DOUBLE NULL,
PRIMARY KEY (id)
);

LOAD DATA LOCAL INFILE '/Users/edenng/Documents/SQL Projects/covid_19/datasets/covid_vaccinations.csv'
INTO TABLE Portfolio_Project.covid_vaccinations
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
;

USE Portfolio_Project;

CREATE TABLE covid_deaths(
id INT NOT NULL,
iso_code TEXT,
continent TEXT,
location TEXT,
date TEXT,
population DOUBLE,
total_cases DOUBLE,
new_cases DOUBLE,
new_cases_smoothed DOUBLE,
total_deaths DOUBLE,
new_deaths DOUBLE,
new_deaths_smoothed DOUBLE,
total_cases_per_million DOUBLE,
new_cases_per_million DOUBLE,
new_cases_smoothed_per_million DOUBLE,
total_deaths_per_million DOUBLE,
new_deaths_per_million DOUBLE,
new_deaths_smoothed_per_million DOUBLE,
reproduction_rate DOUBLE,
icu_patients DOUBLE,
icu_patients_per_million DOUBLE,
hosp_patients DOUBLE,
hosp_patients_per_million DOUBLE,
weekly_icu_admissions DOUBLE,
weekly_icu_admissions_per_million DOUBLE,
weekly_hosp_admissions DOUBLE,
weekly_hosp_admissions_per_million DOUBLE,
PRIMARY KEY (id)
);

LOAD DATA LOCAL INFILE '/Users/edenng/Documents/SQL Projects/covid_19/datasets/covid_deaths.csv'
INTO TABLE Portfolio_Project.covid_deaths
FIELDS TERMINATED BY ','
ENCLOSED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
;

-- CONVERT TEXT TO DATE
-- TURN OFF SAFETY MODE
SET SQL_SAFE_UPDATES = 0;

UPDATE Portfolio_Project.covid_vaccinations
SET date = STR_TO_DATE(date, '%m/%d/%Y');

UPDATE Portfolio_Project.covid_deaths
SET date = STR_TO_DATE(date, '%m/%d/%Y');

-- TURN ON SAFETY MODE
SET SQL_SAFE_UPDATES = 1;

-- PERCENTAGE OF DEATHS IF INFECTED ON GLOBAL SCALE (ENTIRE PERIOD COMBINED)
SELECT SUM(new_deaths) AS last_recorded_total_deaths,
SUM(new_cases) AS last_recorded_total_cases, 
(SUM(new_deaths) / SUM(new_cases)) * 100 AS pct_deaths_if_infected
FROM Portfolio_Project.covid_deaths
WHERE continent NOT LIKE ''
ORDER BY 1
;

-- PERCENTAGE OF DEATHS IF INFECTED ON GLOBAL SCALE (TIMESERIES)
SELECT date, 
SUM(new_deaths) AS total_deaths,
SUM(new_cases) AS total_cases, 
(SUM(new_deaths) / SUM(new_cases)) * 100 AS pct_deaths_if_infected
FROM Portfolio_Project.covid_deaths
WHERE continent NOT LIKE ''
GROUP BY date
ORDER BY 1
;

-- PERCENTAGE OF DEATHS IF INFECTED PER CONTINENT (TIMESERIES)
SELECT location, date, total_deaths, total_cases, 
(total_deaths / total_cases) * 100 AS pct_deaths_if_infected
FROM Portfolio_Project.covid_deaths
WHERE continent LIKE ''
GROUP BY location, date, total_deaths, total_cases
ORDER BY 1,2
;

-- PERCENTAGE OF DEATHS IF INFECTED PER COUNTRY (TIMESERIES)
SELECT location, date, total_deaths, total_cases, 
(total_deaths / total_cases) * 100 AS pct_deaths_if_infected
FROM Portfolio_Project.covid_deaths
WHERE continent NOT LIKE ''
ORDER BY 1,2
;


-- PERCENTAGE OF POPULATION INFECTED PER CONTINENT (TIMESERIES)
SELECT location, date, total_cases, population,
(total_cases / population) * 100 AS pct_of_population_infected
FROM Portfolio_Project.covid_deaths
WHERE continent LIKE ''
GROUP BY location, date, total_cases, population
ORDER BY 1,2
;

-- PERCENTAGE OF POPULATION INFECTED PER COUNTRY (TIMESERIES)
SELECT location, date, total_cases, population,
(total_cases / population) * 100 AS pct_of_population_infected
FROM Portfolio_Project.covid_deaths
WHERE continent NOT LIKE ''
ORDER BY 1,2
;

-- LAST RECORDED PERCENTAGE OF POPULATION INFECTED PER CONTINENT
SELECT location, population,
SUM(new_cases) AS last_recorded_total_cases, 
SUM(new_cases / population) * 100 AS lr_pct_of_population_infected
FROM Portfolio_Project.covid_deaths
WHERE continent LIKE ''
GROUP BY location, population
ORDER BY lr_pct_of_population_infected DESC
;

-- LAST RECORDED PERCENTAGE OF POPULATION INFECTED PER COUNTRY
SELECT location, population,
SUM(new_cases) AS last_recorded_total_cases, 
SUM(new_cases / population) * 100 AS lr_pct_of_population_infected
FROM Portfolio_Project.covid_deaths
WHERE continent NOT LIKE ''
GROUP BY location, population
ORDER BY lr_pct_of_population_infected DESC
;

-- LAST RECORDED TOTAL DEATHS PER CONTINENT
SELECT location, 
MAX(total_deaths) AS max_total_deaths
FROM Portfolio_Project.covid_deaths
WHERE continent LIKE ''
GROUP BY location
ORDER BY max_total_deaths DESC
;

-- LAST RECORDED TOTAL DEATHS PER COUNTRY
SELECT location, 
MAX(total_deaths) AS max_total_deaths
FROM Portfolio_Project.covid_deaths
WHERE continent NOT LIKE ''
GROUP BY location
ORDER BY max_total_deaths DESC
;

-- POPULATION VACCINATED (TIMESERIES)
-- ROLLOVER SUM OF NEW VACCINATIONS COUNT PER DAY
SELECT deaths.continent, deaths.location, deaths.date, deaths.population, 
vaccinations.new_vaccinations,
SUM(vaccinations.new_vaccinations) OVER (PARTITION BY deaths.location 
										ORDER BY deaths.location,
										deaths.date) AS rolling_sum_new_vaccinations
FROM Portfolio_Project.covid_deaths deaths
JOIN Portfolio_Project.covid_vaccinations vaccinations
	ON deaths.location = vaccinations.location
	AND deaths.date = vaccinations.date
WHERE deaths.continent NOT LIKE ''
ORDER BY 2,3
;

-- PERCENTAGE POPULATION VACCINATED (TIMESERIES)
-- PERFORM FURTHER CALCULATIONS USING rolling_sum_new_vaccinations
-- USE CTE 
WITH pct_pop_vac (
continent, 
location, 
date,
population, 
new_vaccinations,
rolling_sum_new_vaccinations)
AS(
SELECT deaths.continent, 
deaths.location, 
deaths.date,
deaths.population, 
vaccinations.new_vaccinations,
SUM(vaccinations.new_vaccinations) OVER (
PARTITION BY deaths.location
ORDER BY deaths.location,
deaths.date
) AS rolling_sum_new_vaccinations
FROM Portfolio_Project.covid_deaths deaths
JOIN Portfolio_Project.covid_vaccinations vaccinations
	ON deaths.location = vaccinations.location
	AND deaths.date = vaccinations.date
WHERE deaths.continent NOT LIKE ''
)
SELECT *, (rolling_sum_new_vaccinations / population) * 100 AS pct_population_vaccinated
FROM pct_pop_vac;


-- USING TEMP TABLE

DROP TABLE IF EXISTS Portfolio_Project.pct_pop_vac_table;

CREATE TEMPORARY TABLE Portfolio_Project.pct_pop_vac_table
SELECT deaths.continent, 
deaths.location, 
deaths.date,
deaths.population, 
vaccinations.new_vaccinations,
SUM(vaccinations.new_vaccinations) OVER (
PARTITION BY deaths.location ORDER BY deaths.location,
deaths.date) 
AS rolling_sum_new_vaccinations
FROM Portfolio_Project.covid_deaths deaths
JOIN Portfolio_Project.covid_vaccinations vaccinations
	ON deaths.location = vaccinations.location
	AND deaths.date = vaccinations.date
WHERE deaths.continent NOT LIKE '';

SELECT *, (rolling_sum_new_vaccinations / population) * 100 AS pct_population_vaccinated
FROM Portfolio_Project.pct_pop_vac_table;


-- CREATE VIEW TO STORE FOR VISUALIZATIONS
DROP VIEW IF EXISTS Portfolio_Project.pct_pop_vac_view;

CREATE VIEW Portfolio_Project.pct_pop_vac_view AS
WITH pct_pop_vac (
continent, 
location, 
date,
population, 
new_vaccinations,
rolling_sum_new_vaccinations)
AS(
SELECT deaths.continent, 
deaths.location, 
deaths.date,
deaths.population, 
vaccinations.new_vaccinations,
SUM(vaccinations.new_vaccinations) OVER (
PARTITION BY deaths.location
ORDER BY deaths.location,
deaths.date
) AS rolling_sum_new_vaccinations
FROM Portfolio_Project.covid_deaths deaths
JOIN Portfolio_Project.covid_vaccinations vaccinations
	ON deaths.location = vaccinations.location
	AND deaths.date = vaccinations.date
WHERE deaths.continent NOT LIKE ''
)
SELECT *, (rolling_sum_new_vaccinations / population) * 100 AS pct_population_vaccinated
FROM pct_pop_vac;