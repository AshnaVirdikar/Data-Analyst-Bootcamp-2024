-- EDA

-- Here we are just going to explore the data and find trends or patterns or anything interesting like outliers

-- normally when you start the EDA process you have some idea of what you're looking for

-- with this info we are just going to look around and see what we find!

SELECT * 
FROM covid_layoffs.layoffs_staging2;

-- EASIER QUERIES

#lets find out min , max and average layoffs for companies


SELECT 
    company,
    location,
    industry,
    total_laid_off,`date`
FROM 
    covid_layoffs.layoffs_staging2
WHERE 
    total_laid_off = (SELECT MAX(total_laid_off) FROM layoffs_staging2); #12000 from GOOGLE WOAH! in 2023 
    
SELECT max(total_laid_off ), max(percentage_laid_off) from layoffs_staging2;
SELECT avg(layoffs_staging2.total_laid_off) as avg_layoffs
FROM covid_layoffs.layoffs_staging2
;


-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM covid_layoffs.layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM  covid_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;
-- these are mostly startups it looks like who all went out of business during this time 116 companies total
SELECT *
FROM  covid_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY total_laid_off DESC; #Twitter laid off 3700 employees in 2022 WOW!!

-- if we order by funds_raised_millions we can see how big some of these companies were
SELECT *
FROM  covid_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC; #Again, Twitter raised 1.29 Billion!! 221 companies went completely under in total


# HOOQ raised 95 million and went bankrupt wow !


-- SOMEWHAT TOUGHER AND MOSTLY USING GROUP BY--------------------------------------------------------------------------------------------------

-- Companies with the biggest single Layoff

SELECT company, total_laid_off, country
FROM  covid_layoffs.layoffs_staging
ORDER BY 3 DESC
LIMIT 5;   

-- now that's just on a single day

-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM  covid_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;



-- by location
SELECT location, SUM(total_laid_off)
FROM  covid_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- this it total in the past 3 years or in the dataset

SELECT country, SUM(total_laid_off)
FROM  covid_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM  covid_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;


SELECT industry, SUM(total_laid_off)
FROM  covid_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;


SELECT stage, SUM(total_laid_off)
FROM  covid_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

# lets look at the percentages laid off 
SELECT company, avg(percentage_laid_off)
FROM  covid_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;



-- TOUGHER QUERIES------------------------------------------------------------------------------------------------------------------------------------

-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year. It's a little more difficult.
-- I want to look at 
#Rolling total of layoffs by month 
SELECT SUBSTRING(`date`,1,7) as `month`, sum(total_laid_off) 
from layoffs_staging2
where SUBSTRING(`date`,1,7) is not null
group by `month`
order by 1 asc;

# do a rolling sum of the output above using a cte
-- Rolling Total of Layoffs Per Month
with Rolling_Total as 
(SELECT SUBSTRING(`date`,1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where SUBSTRING(`date`,1,7) is not null
group by `month`
order by 1 asc)
SELECT `month`,total_off, sum(total_off)  over(order by `month`) as rolling_total
from Rolling_Total;

# lets find out how many companies laid off employees by the YEAR
SELECT company,YEAR(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, YEAR(`date`)
order by company asc;


SELECT company,YEAR(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, YEAR(`date`)
order by 3 desc;


#me
WITH Company_Year (company, years, total_laid_off) AS 
(
  SELECT company, YEAR(`date`), SUM(total_laid_off) 
  FROM layoffs_staging2
  GROUP BY company, YEAR(`date`)
) 
SELECT*, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) as RANKING
FROM Company_Year where years is not null order by RANKING;

#now lets filter down on RANKING & get TOP 5 company layoffs per YEAR

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 5
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;


-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

-- now use it in a CTE so we can query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;



