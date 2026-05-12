-- Databricks notebook source
--- See the information in the dataset
---Total 13 columns
SELECT * 
FROM `workspace`.`case_study3`.`car_sales_data` 
LIMIT 100;
-----------------------------------------------------------------------------Exploratory Data Analysis---------------------------------------------------------
--- Check if data types in the columns
--Incorrect data types for: year column is an integer (its fine because it only contains yyyy values) and the saledate column is a string: 'dayname MMM dd yyyy HH:mm:ss' (it must be converted to date type)
SELECT * 
FROM `workspace`.`case_study3`.`car_sales_data` 
LIMIT 100;

--Total rows are 558811 
SELECT COUNT(*) AS number_of_data
FROM `workspace`.`case_study3`.`car_sales_data` 
LIMIT 100;
--------------------------------------------------------------------Check for NULL values
------------------------------------------------------------Check for null values in the Qualitative columns
-- There are NULL values in columns for: make (10301), model (10399)
SELECT *
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE make IS NULL OR model is null or body is null or state is null or seller is null
LIMIT 100;

--- count make NULLs
SELECT COUNT(*) as make_null
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE make IS NULL;

---count model NULLs
SELECT COUNT(*)
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE model is null;

-----------------------------------------------------------Check for NULL values in Quantitative columns
---There are NULL values in odometer (94), mmr(12) and sellingprice (12)
SELECT *
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE odometer IS NULL OR mmr is null or sellingprice is null 
LIMIT 100;
---count odometer NULLs
SELECT COUNT(*)
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE odometer IS NULL;
---count mmr NULLs
SELECT COUNT(*)
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE mmr IS NULL;
---count selingprice NULLs
SELECT COUNT(*)
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE sellingprice IS NULL;

--------------------------------------------------------------Check for null values in the date columns
---There are 12 null values in the saledate column, and no NULL values in the year column (can convert year column from integer to date and use it to replace the null values in the saledate column)
SELECT *
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE year IS NULL OR saledate is null 
LIMIT 100;

SELECT COUNT(*)
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE saledate is null;
----------------------------------------------------------------Check the unique values, especially for columns with NULL values
---96 unique make, including NULL
SELECT COUNT(DISTINCT make)
FROM `workspace`.`case_study3`.`car_sales_data` 
LIMIT 100;

---973 unique models
SELECT COUNT(DISTINCT model)
FROM `workspace`.`case_study3`.`car_sales_data` 
LIMIT 100;

--- 86 unique body
SELECT COUNT(DISTINCT body)
FROM `workspace`.`case_study3`.`car_sales_data` 
LIMIT 100;

--- 38 unique states
SELECT COUNT(DISTINCT state)
FROM `workspace`.`case_study3`.`car_sales_data` 
LIMIT 100;
---------------------------------------------------------------- 14261 unique sellers 
SELECT COUNT(DISTINCT seller)
FROM `workspace`.`case_study3`.`car_sales_data` 
LIMIT 100;
----------------------------------------------------------------Check the Minimum and maximum odometer, mmr, sellingprice
---lowest mileage is 1, and highest mileage is 999999
SELECT MIN(odometer) AS lowest_mileage,
MAX(odometer) AS highest_mileage
FROM `workspace`.`case_study3`.`car_sales_data`;

--- lowest mmr is 25, highest mmr is 182000
SELECT MIN(mmr) AS lowest_mmr,
MAX(mmr) AS highest_mmr
FROM `workspace`.`case_study3`.`car_sales_data`;

--- lowest selling price is 1, highest selling price is 230000
SELECT MIN(sellingprice) AS lowest_price,
MAX(sellingprice) AS highest_price
FROM `workspace`.`case_study3`.`car_sales_data`;

------------------------------------------------------------------Check the Date ranges
---minimum year of manufacture is 1982, maximum manufacture year is 2015
SELECT MIN(year) AS minimum_year,
MAX(year) AS maximum_year
FROM `workspace`.`case_study3`.`car_sales_data`;

-----------------------------------------------------------------------------------Data Cleaning------------------------------------------------------------------
------------------------------------------------------------Change year integer type to DATE type (to enable it to replace nulls in the sale date) using alter table and make_date
---1. Create a new empty column for Date type
--ALTER TABLE `workspace`.`case_study3`.`car_sales_data` ADD COLUMN year_manufacture_datetype DATE;

---2. Add data from the year column into the new column
UPDATE `workspace`.`case_study3`.`car_sales_data`
SET year_manufacture_datetype = make_date(year, 1, 1);

-------------------------------------------------------------Change saledate string type to DATE type, using Substring, alter table and to_date
-- Convert saledate string type to Date type and extract the date only
---1. Create a new empty column for data type
--ALTER TABLE `workspace`.`case_study3`.`car_sales_data` ADD COLUMN saledate_datetype DATE;

---2. ADD data from the saledate column into the new column
UPDATE `workspace`.`case_study3`.`car_sales_data`
SET saledate_datetype = TO_DATE(SUBSTR(saledate, 5), 'MMM dd yyyy HH:mm:ss');

------------------------------------------------------------ Deal with NULL values in the columns
------------------------ Replace NULL values in the saledate_datetype column with values from the year_manufacture_datetype column
UPDATE `workspace`.`case_study3`.`car_sales_data`
SET saledate_datetype = COALESCE(saledate, year_manufacture_datetype)
WHERE saledate IS NULL;
---check for NULL in the updated column- No NULLs
SELECT COUNT(*) 
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE saledate_datetype IS NULL;

---------------------------------- Replace NULL values in the Qualitative columns
------------------------Replace NULL values in the make column
UPDATE `workspace`.`case_study3`.`car_sales_data` 
SET make = 'Unkown'
WHERE make IS NULL;
---check for NULL in the updated column- No NULLs
SELECT COUNT(*) 
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE make IS NULL;

---------------------- Replace NULL values in the model column
UPDATE `workspace`.`case_study3`.`car_sales_data` 
SET model = 'Unkown'
WHERE model IS NULL;
---check for NULL in the updated column- No NULLs
SELECT COUNT(*)
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE model is null;

--------------------- Replace NULL values in the Quantitative columns 
---1. Calculate the average odometer (68324), mmr (13771) and sellingprice (13613) of NONE-NULL values

SELECT ROUND(AVG(odometer),0) AS average_odometer,
    ROUND(AVG(mmr),0) AS average_mmr,
    ROUND(AVG(sellingprice),0) AS average_sellingprice
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE sellingprice IS NOT NULL AND mmr IS NOT NULL AND odometer IS NOT NULL
LIMIT 100;

----2. Replace the NULLS with the averages: odometer, mmr and sellingprice columns
---------------------- Replace NULL values in the odometer column
UPDATE `workspace`.`case_study3`.`car_sales_data` 
SET odometer = 68324
WHERE odometer IS NULL;
---check for NULL in the updated column- No NULLs
SELECT COUNT(*)
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE odometer is null;

---------------------- Replace NULL values in the mmr column
UPDATE `workspace`.`case_study3`.`car_sales_data` 
SET mmr = 13771
WHERE mmr IS NULL;
---check for NULL in the updated column- No NULLs
SELECT COUNT(*)
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE mmr is null;

---------------------- Replace NULL values in the sellingprice column
UPDATE `workspace`.`case_study3`.`car_sales_data` 
SET sellingprice = 13613
WHERE sellingprice IS NULL;
---check for NULL in the updated column- No NULLs
SELECT COUNT(*)
FROM `workspace`.`case_study3`.`car_sales_data` 
WHERE sellingprice is null;

----------------------------------------------------------------------------------------Calculate units sold
---step 1.Add the new empty column for integer type
---ALTER TABLE `workspace`.`case_study3`.`car_sales_data` ADD COLUMN units_sold INT;

-- Step 2: Assume that 1 row = 1 unit sold
-- Step 3: Update the table and set the units sold to 1
UPDATE `workspace`.`case_study3`.`car_sales_data`
SET units_sold = 1;

-----------------------------------------------------------------------------Create new features, Aggregate the data-------------------------------------------------------------------------
SELECT
----Qualitative columns
    make,
    model,
    state,
---Date columns
    YEAR(year_manufacture_datetype) AS year_manufacture,
    saledate_datetype,
    YEAR(saledate_datetype) AS sale_year,
    MONTH(saledate_datetype) AS sale_month,
    MONTHNAME(saledate_datetype) AS month_name,
    QUARTER(saledate_datetype) AS sale_quarter,

    CASE
        WHEN QUARTER(saledate_datetype) = 1 THEN 'Quarter 1'
        WHEN QUARTER(saledate_datetype) = 2 THEN 'Quarter 2'
        WHEN QUARTER(saledate_datetype) = 3 THEN 'Quarter 3'
        WHEN QUARTER(saledate_datetype) = 4 THEN 'Quarter 4'
    END AS quarter_name,

    
-------Aggregate the Qualitative columns
    SUM(odometer) AS total_mileage,
    SUM(mmr) AS total_cost_price,
    SUM(units_sold) AS total_units_sold,
    SUM(sellingprice) AS totalselling_price,

-- Calculate the Total Revenue
    SUM(sellingprice * units_sold) AS total_revenue, 
--Calculate the Profit Margin
    ROUND((SUM(sellingprice - mmr ) / SUM(sellingprice)) * 100, 2) AS profit_margin,  

    -- Performance Tiers
    CASE 
        WHEN ROUND((SUM(sellingprice - mmr ) / SUM(sellingprice)) * 100, 2) <= 5 THEN 'Low Margin'
        WHEN ROUND((SUM(sellingprice - mmr ) / SUM(sellingprice)) * 100, 2) BETWEEN 6 AND 15 THEN 'Medium Margin'
        ELSE 'High Margin'
    END AS performance_tiers

FROM `workspace`.`case_study3`.`car_sales_data`
GROUP BY make,
    model,
    state,
    year_manufacture,
    saledate_datetype,
    sale_year,
    sale_month,
    month_name,
    sale_quarter,
    quarter_name;
