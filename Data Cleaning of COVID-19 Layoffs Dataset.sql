-- SQL Project - Data Cleaning
#clean the data -2931 records imported data from 2020 to 2023


SELECT * 
FROM covid_layoffs.layoffs_git;


-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE layoffs_staging 
LIKE covid_layoffs.layoffs_git;

INSERT layoffs_staging 
SELECT * FROM covid_layoffs.layoffs_git;


-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

-- 1. Remove Duplicates

# First let's check for duplicates

SELECT *
FROM layoffs_staging
;  
#REMOVE DUPLICATES
#adding row numbers to delete dupicates using window function using row_number
SELECT*,
	ROW_NUMBER() OVER (
		partition by company, industry, total_laid_off, percentage_laid_off,`date`) AS row_num
	FROM covid_layoffs.layoffs_staging ;


        
#Create a CTE for duplicates
WITH duplicates_cte as
(SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company,industry, total_laid_off,percentage_laid_off,`date`) AS row_num
	FROM 
		covid_layoffs.layoffs_staging)
        SELECT*
        FROM duplicates_cte
        WHERE row_num >1;
    
-- let's just look at Company oda to confirm

SELECT *
FROM covid_layoffs.layoffs_staging
WHERE company = 'Casper'
;  # we can see the dupes


-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- these are our real duplicates 


SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company , location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		covid_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
    
#check again for company 'Yahoo'  
SELECT *
FROM covid_layoffs.layoffs_staging
WHERE company = 'Yahoo'
;

-- these are the ones we want to delete where the row number is > 1 or 2 or greater essentially

-- now you may want to write it like this:
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company
 , location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company
 , location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		covid_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;

#create a new table to delete dupes
#DROP TABLE IF EXISTS layoffs_staging2;
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

#inserting all the values in layoffs_staging 2 table using insert statement

INSERT INTO  layoffs_staging2
SELECT*,
ROW_NUMBER() OVER (
			PARTITION BY company, location, 
            industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM covid_layoffs.layoffs_staging;
	
    SELECT* FROM layoffs_staging2
    WHERE row_num >1;
    
   #now that we identified our dupers we will delete them!   
DELETE FROM layoffs_staging2
    WHERE row_num >1;
    
    SELECT* FROM layoffs_staging2; #all duplicate copies are deleted
    
    -- 2. Standardize Data
SELECT Distinct company FROM layoffs_staging2;
SELECT company, trim(company)
FROM covid_layoffs.layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM  covid_layoffs.layoffs_staging2
ORDER BY industry;   #we find there are some null values

SELECT DISTINCT industry
FROM  covid_layoffs.layoffs_staging2
ORDER BY 1; 

#checking to see if we ahve null values in industry column 
SELECT *
FROM covid_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;  #we get 3 null values Airbnb, Juul, Carvana

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';
-- nothing wrong here
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE covid_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';
-- now if we check those are all null

SELECT *
FROM covid_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM covid_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------
-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
SELECT DISTINCT industry
FROM covid_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- now that's taken care of:
SELECT DISTINCT industry
FROM covid_layoffs.layoffs_staging2
ORDER BY industry;

-- --------------------------------------------------
-- we also need to look at 
 SELECT *
FROM  covid_layoffs.layoffs_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM  covid_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM  covid_layoffs.layoffs_staging2
ORDER BY country;


-- Let's also fix the date columns:      

SELECT *
FROM covid_layoffs.layoffs_staging2;

-- we can use str to date to update this field

update layoffs_staging2
set `date` = NULL
where `date` = 'None' OR '  ';
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM covid_layoffs.layoffs_staging2;

-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values


-- 4. remove any columns and rows we need to

SELECT *
FROM covid_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM covid_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM covid_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM covid_layoffs.layoffs_staging2;

ALTER TABLE covid_layoffs.layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM covid_layoffs.layoffs_staging2;



----- done cleaning data now we start eda



