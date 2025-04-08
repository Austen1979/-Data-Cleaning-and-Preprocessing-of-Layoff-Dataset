 -- Data Cleaning Process

-- Retrieve all records from the raw layoffs table for an initial overview.

select * 
from layoffs;

-- Steps for Data Cleaning:
-- 1. Remove duplicates 
-- 2. Standardize data (e.g., company names, industries, dates, countries)
-- 3. Handle null or blank values
-- 4. Remove unnecessary columns 


-- Avoid making changes to the raw table; instead, create a staging table.


-- Create a new staging table with the same structure as layoffs.
create table Layoffs_staging 
Like layoffs;

-- Verify the new table structure.
select * 
from layoffs_staging;

-- Insert data from the original table into the staging table.
Insert Layoffs_staging
select *
from layoffs;

-- Verify the inserted data.
select *
from layoffs_staging;




-- Removing Duplicates
-- Identify duplicate records based on key attributes.
select *,
ROW_NUMBER() OVER( 
PARTITION BY COMPANY, INDUSTRY, TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF,`DATE`) AS ROW_NUM 
from layoffs_staging;


-- Identify all duplicates using a Common Table Expression (CTE).
WITH DUPLICATE_CTE AS 
(

select *,
ROW_NUMBER() OVER( 
PARTITION BY COMPANY,LOCATION, INDUSTRY, TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF,`DATE`,STAGE,COUNTRY,funds_raised_millions) AS ROW_NUM 
from layoffs_staging
)
SELECT * 
FROM DUPLICATE_CTE
WHERE ROW_NUM >1; 


-- Check for duplicate records of a specific company.
SELECT * 
FROM layoffs_staging
WHERE company ='CASPER';



-- THE PROCESS TO REMOVE DUPLICATE DATA 

-- Create a new cleaned staging table to store deduplicated records.
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
  `ROW_NUM` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Verify the new staging table

SELECT * 
FROM layoffs_staging2;


-- Insert unique records into the new staging table with row numbers assigned.
INSERT INTO layoffs_staging2
select *,
ROW_NUMBER() OVER( 
PARTITION BY COMPANY,LOCATION, INDUSTRY, TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF,`DATE`,STAGE,COUNTRY,funds_raised_millions) AS ROW_NUM 
from layoffs_staging;


-- Delete duplicate records (where ROW_NUM > 1).
DELETE
FROM layoffs_staging2
WHERE ROW_NUM >1;




-- Verify that duplicates have been removed.
SELECT *
FROM layoffs_staging2
WHERE ROW_NUM >1 ; 



SELECT * 
FROM layoffs_staging2;

-- Standardizing Data

-- Trim company names to remove unnecessary spaces.

SELECT COMPANY,trim(COMPANY)
FROM LAYOFFS_STAGING2 ;


-- Update company names to ensure consistent formatting.
UPDATE LAYSOFF_STAGING2 
SET COMPANY= TRIM(COMPANY);

-- Standardize industry names, particularly correcting variations of 'Crypto'.
select *
FROM layoffs_staging2
where industry like 'crypto%';
update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%';


-- Standardize country names (removing trailing periods from 'United States.')

select distinct country, trim( trailing '.' from country)
from layoffs_staging2
where country like 'united states%'
order by 1;

update layoffs_staging2
set country = trim( trailing '.' from country)
where country  like ' united states%';

-- Formatting the Date Column

-- Convert date strings to proper date format.

select `date`, str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

-- Change the column type to DATE for proper sorting and filtering.
update layoffs_staging2 
set `date` = str_to_date(`date`,'%m/%d/%Y');

-- Handling Null and Missing Values

-- Identify records where total layoffs and percentage laid off are missing.

Alter table layoffs_staging2 
modify column `date` date; 



select * 
from layoffs_staging2
where total_laid_off  is null and percentage_laid_off is null;

-- Identify missing industry values.
select *
from layoffs_staging2
where industry is null or industry = '';


select *
from layoffs_staging2
where company = 'airbnb';

-- Find other companies with missing industry information.
select t1.industry, t2.industry
from layoffs_staging2  t1
join layoffs_staging2  t2 
on t1.company = t2.company 
where (t1.industry is null or t1.industry='') and 
 t2.industry is not null;
 
 -- Update blank industry values to NULL.
 update layoffs_staging2
 set industry = null 
 where industry = '';
 
 
-- Fill in missing industry values using data from the same company.
update layoffs_staging2 t1 
join layoffs_staging2  t2 
on t1.company = t2.company
set t1.industry = t2.industry 
where t1.industry is null and 
 t2.industry is not null;
 
 -- Verify the cleaned table.
 select *
 from layoffs_staging2;
 
 
 
 -- Identify and remove records with missing critical information.
 select * 
from layoffs_staging2
where total_laid_off  is null and percentage_laid_off is null;

 
 # deleted data missing information 
 delete
 from layoffs_staging2
where total_laid_off  is null and percentage_laid_off is null;

 -- Removing Unnecessary Columns

-- Verify the table before dropping columns.
select *
from layoffs_staging2;

-- Drop the ROW_NUM column as it is no longer needed.
Alter table layoffs_staging2
drop column ROW_NUM




