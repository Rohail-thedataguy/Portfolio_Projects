-- Data Cleaning in SQL


SELECT * FROM world_layoffs.layoffs;



-- Creating Staging Table. Will work on Staging Table. Preserving Raw Table in case something goes wrong.

CREATE TABLE layoffs_staging
LIKE world_layoffs.layoffs;

SELECT * FROM layoffs_staging;

INSERT layoffs_staging
SELECT * FROM world_layoffs.layoffs;

SELECT * FROM layoffs_staging;


-- Order of Data Cleaning followed in the Project
-- 1. Removing Duplicates
-- 2. Standardize the Data
-- 3. Dealing with Null Values
-- 4. Remove any Colums (Not Necessary for Analysis)


-- 1.Removing Duplicates

#Checking for Duplicates

SELECT * FROM layoffs_staging;


SELECT *, ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging;

-- Creating a CTE

WITH duplicate_cte AS
(
SELECT *, ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging
)

SELECT *
FROM duplicate_cte
WHERE row_num > 1;
 
 
SELECT *
FROM layoffs_staging
WHERE company In ('Casper', 'Yahoo');

-- Creating another table with row_num column in it

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

Select *
from layoffs_staging2;

-- populating the table
INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging;

-- Finding the Duplicates
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;
#5 Duplicates found

-- Removing the Duplicates
DELETE
FROM layoffs_staging2
WHERE row_num > 1;
#5 Rows were affected


-- 2.Standardizing Data


#Using Trim function to remove the Blank space at the start and end of the text
SELECT company, trim(company)
From layoffs_staging2;


UPDATE layoffs_staging2
SET company = trim(company);
#11 rows were affected

-- Checking for discrepancy in industry names
SELECT distinct industry
FROM layoffs_staging2
ORDER BY 1;


SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Updating discrepancy in Industry name
UPDATE layoffs_staging2
SET	industry = 'Crypto'
WHERE industry LIKE 'Crypto%';
# 3 rows affected


SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- Found 2 United States, one with dot at the end

UPDATE layoffs_staging2
SET	country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';
#4 rows were affected

-- Formatting Date field and Changing it to 'date' data type from 'text'
SELECT `date`, str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`,'%m/%d/%Y');

#2355 rows affected

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;



-- 3.Dealing with NULL values

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

-- populating missing data where possible
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Experimenting
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';
#nothing wrong here

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';
#some industry rows are populated and some not


-- Setting the blanks to NULL becuase NULLs are easier to work with
UPDATE world_layoffs.layoffs_staging2 
SET industry = NULL
WHERE industry = '';

SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Populating the NULLs where possible

UPDATE layoffs_staging2 as table1
JOIN layoffs_staging2 as table2
	ON table1.company = table2.company
SET table1.industry = table2.industry
WHERE table1.industry IS NULL
AND table2.industry IS NOT NULL;


SELECT *
FROM world_layoffs.layoffs_staging2;


SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Deleting Useless data that cannot be used for analysis purposes

DELETE 
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- 4. Removing columns
-- Dropping the row_num column that was created to remove the duplicates

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM world_layoffs.layoffs_staging2;