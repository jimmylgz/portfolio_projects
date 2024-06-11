# Data Cleaning By Li Guanzhen

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove Any Columns


-- STEP 0, 避免动到原数据，弄一个同样的数据拷贝来操作，以下只是把数据结构拷贝了。
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

-- 这里把数据插入新建的数据表
INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- Step 1, removing duplicates
-- if there is a number that is 2 or above, then there is an issue
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;


-- 用CTE, common table expression
WITH duplicate_cte AS
(
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


SELECT *
FROM layoffs_staging
WHERE company = 'Casper';



-- 由于我们没法update或者delete一个cte, 因此我们创建一个新的table

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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



-- 在新表里插入含row number的内容
INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- Step 2, Standardizing data，将数据标准化
-- 去除头尾多余的空格
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- now deal with the industry, order by 1 means order by the first column
-- you have same meaning but with different spelling,e .g. crypto currency, cryptocurrency are the same as crypto
-- there is null value, deal with it later
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
WHERE country LIKE 'United States%';

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2;

-- change the date column's type from text to date, important function STR_TO_DATE
SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

-- we now need to change to the date type to be `date`
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- there's null value, deal with it by populating it later
SELECT *
FROM layoffs_staging2;

-- Step 3, remove null and blank values
-- deal with total_laid_off and percentage_laid_off columns
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

-- deal with industry column
SELECT *
FROM layoffs_staging2
WHERE industry is NULL
OR industry ='';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- same company that has industry but only shown on some rows, populate the values that exist
-- 巧用self join可以填充空白值
-- set the blank values in industry to be null
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
    AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- At this point, we cannot populate the null data anymore （无法再填充）
SELECT *
FROM layoffs_staging2;

-- if you are confident to delete some values according to the needs, we can run the following to delete the unwanted rows
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- also, you don't need the column of num, you can now delete the column by the following command
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;