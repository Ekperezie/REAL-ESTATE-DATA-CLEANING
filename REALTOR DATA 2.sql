-- DATA CLEANING OF REALTOR DATA

-- [STEP 1]  CREATION OF SIMILAR TABLE
 -- CREATE TABLE realtor_data_2
  -- LIKE `realtor-data.zip`;

-- INSERT realtor_data_2
-- SELECT * FROM `realtor-data.zip`;

-- [STEP 2] REMOVING DUPLICATES

SELECT *,
 row_number() OVER(
 PARTITION BY brokered_by, `status`, price, bedroom, bathroom, acre_lot, street, city ) as row_num FROM realtor_data_2;

WITH duplicate_cte as(
SELECT *,
 row_number() OVER(
 PARTITION BY brokered_by, `status`, price, bedroom, bathroom, acre_lot, street, city ) as row_num FROM realtor_data_2
)
SELECT* FROM duplicate_cte
WHERE row_num > 1;
 
 
 CREATE TABLE `realtor_data_2a` (
  `brokered_by` double DEFAULT NULL,
`status` text,
  `price` double DEFAULT NULL,
  `bedroom` int DEFAULT NULL,
  `bathroom` int DEFAULT NULL,
  `acre_lot` double DEFAULT NULL,
  `street` double DEFAULT NULL,
  `city` text,
  `state` text,
  `zip_code` text,
  `house_size` text,
  `prev_sold_date` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM realtor_data_2a;
INSERT INTO realtor_data_2a
SELECT *,
 row_number() OVER(
 PARTITION BY brokered_by, `status`, price, bedroom, bathroom, acre_lot, street, city ) as row_num FROM realtor_data_2;
 
 DELETE 
 FROM realtor_data_2a
 WHERE row_num > 1;
 
 SELECT * FROM realtor_data_2a;
 
 
 -- [step 3 ] STANDARDIZING THE DATA
 
 SELECT brokered_by, trim(brokered_by)
 FROM realtor_data_2a;
 
 UPDATE realtor_data_2a
 SET brokered_by = Trim(brokered_by);
 
SELECT DISTINCT City
 FROM realtor_data_2a
 ORDER BY 1;
 
 SELECT DISTINCT State
 FROM realtor_data_2a
 ORDER BY 1;

 SELECT DISTINCT `Status`
 FROM realtor_data_2a
 ORDER BY 1;
 
-- [STEP 4] REMOVING NULL AND BLANK VALUES
SELECT * FROM realtor_data_2a
WHERE brokered_by IS NULL
 OR `BEDROOM` IS NULL OR `bedroom` = ''
 OR `bathroom` IS NULL OR `bathroom` = ''
 OR `acre_lot` IS NULL OR `acre_lot` = ''
 OR `STREET` IS NULL OR `STREET` = ''
 OR `house_size`  IS NULL OR `house_size`
 OR `prev_sold_date` IS NULL OR `prev_sold_date`;
 
 UPDATE realtor_data_2a
 SET `prev_sold_date` = NULL
 WHERE  `prev_sold_date` = '';
 
 UPDATE realtor_data_2a
 SET `house_size` = NULL
 WHERE  `house_size` = '';
 
 -- [STEP 5] REMOVE ANY COLUMNS OR ROWS
 ALTER TABLE  realtor_data_2a
 DROP COLUMN row_num;
 
 SELECT * FROM realtor_data_2a

