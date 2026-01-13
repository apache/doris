-- Create full schema change test tables with complex types (map, struct, array)
-- These tables are used to test complex schema evolution scenarios
-- Tables are created with initial schema, then ALTER TABLE is used to add complex type columns
USE regression_hudi;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS hudi_full_schema_change_parquet;
DROP TABLE IF EXISTS hudi_full_schema_change_orc;

-- Create hudi_full_schema_change_parquet table with initial schema (only id)
CREATE TABLE IF NOT EXISTS hudi_full_schema_change_parquet (
    id INT
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.parquet.small.file.limit = '100',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/hudi_full_schema_change_parquet';

-- Insert initial data (only id)
INSERT INTO hudi_full_schema_change_parquet (id) VALUES
  (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15), (16), (17), (18), (19), (20), (21);

-- Execute schema changes: Add complex type columns step by step
ALTER TABLE hudi_full_schema_change_parquet ADD COLUMNS (
  new_map_column MAP<STRING, STRUCT<full_name: STRING, age: INT, gender: STRING>>,
  struct_column STRUCT<country: STRING, city: STRING, population: INT>,
  array_column ARRAY<STRUCT<item: STRING, quantity: INT, category: STRING>>
);

-- Insert data with new columns
INSERT INTO hudi_full_schema_change_parquet VALUES
  (0, map('person0', named_struct('full_name', 'zero', 'age', 2, 'gender', null)), named_struct('country', null, 'city', 'cn', 'population', 1000000), array(named_struct('item', 'Apple', 'quantity', null, 'category', null), named_struct('item', 'Banana', 'quantity', null, 'category', null))),
  (1, map('person1', named_struct('full_name', 'Alice', 'age', 25, 'gender', null)), named_struct('country', null, 'city', 'New York', 'population', 8000000), array(named_struct('item', 'Apple', 'quantity', null, 'category', null), named_struct('item', 'Banana', 'quantity', null, 'category', null))),
  (2, map('person2', named_struct('full_name', 'Bob', 'age', 30, 'gender', null)), named_struct('country', null, 'city', 'Los Angeles', 'population', 4000000), array(named_struct('item', 'Orange', 'quantity', null, 'category', null), named_struct('item', 'Grape', 'quantity', null, 'category', null))),
  (3, map('person3', named_struct('full_name', 'Charlie', 'age', 28, 'gender', null)), named_struct('country', null, 'city', 'Chicago', 'population', 2700000), array(named_struct('item', 'Pear', 'quantity', null, 'category', null), named_struct('item', 'Mango', 'quantity', null, 'category', null))),
  (4, map('person4', named_struct('full_name', 'David', 'age', 35, 'gender', null)), named_struct('country', null, 'city', 'Houston', 'population', 2300000), array(named_struct('item', 'Kiwi', 'quantity', null, 'category', null), named_struct('item', 'Pineapple', 'quantity', null, 'category', null))),
  (5, map('person5', named_struct('full_name', 'Eve', 'age', 40, 'gender', null)), named_struct('country', 'USA', 'city', 'Phoenix', 'population', 1600000), array(named_struct('item', 'Lemon', 'quantity', null, 'category', null), named_struct('item', 'Lime', 'quantity', null, 'category', null))),
  (6, map('person6', named_struct('full_name', 'Frank', 'age', 22, 'gender', null)), named_struct('country', 'USA', 'city', 'Philadelphia', 'population', 1500000), array(named_struct('item', 'Watermelon', 'quantity', null, 'category', null), named_struct('item', 'Strawberry', 'quantity', null, 'category', null))),
  (7, map('person7', named_struct('full_name', 'Grace', 'age', 27, 'gender', null)), named_struct('country', 'USA', 'city', 'San Antonio', 'population', 1500000), array(named_struct('item', 'Blueberry', 'quantity', null, 'category', null), named_struct('item', 'Raspberry', 'quantity', null, 'category', null))),
  (8, map('person8', named_struct('full_name', 'Hank', 'age', 32, 'gender', null)), named_struct('country', 'USA', 'city', 'San Diego', 'population', 1400000), array(named_struct('item', 'Cherry', 'quantity', 5, 'category', null), named_struct('item', 'Plum', 'quantity', 3, 'category', null))),
  (9, map('person9', named_struct('full_name', 'Ivy', 'age', 29, 'gender', null)), named_struct('country', 'USA', 'city', 'Dallas', 'population', 1300000), array(named_struct('item', 'Peach', 'quantity', 4, 'category', null), named_struct('item', 'Apricot', 'quantity', 2, 'category', null))),
  (10, map('person10', named_struct('full_name', 'Jack', 'age', 26, 'gender', null)), named_struct('country', 'USA', 'city', 'Austin', 'population', 950000), array(named_struct('item', 'Fig', 'quantity', 6, 'category', null), named_struct('item', 'Date', 'quantity', 7, 'category', null))),
  (11, map('person11', named_struct('full_name', 'Karen', 'age', 31, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Seattle', 'population', 750000), array(named_struct('item', 'Coconut', 'quantity', 1, 'category', null), named_struct('item', 'Papaya', 'quantity', 2, 'category', null))),
  (12, map('person12', named_struct('full_name', 'Leo', 'age', 24, 'gender', 'Male')), named_struct('country', 'USA', 'city', 'Portland', 'population', 650000), array(named_struct('item', 'Guava', 'quantity', 3, 'category', null), named_struct('item', 'Lychee', 'quantity', 4, 'category', null))),
  (13, map('person13', named_struct('full_name', 'Mona', 'age', 33, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Denver', 'population', 700000), array(named_struct('item', 'Avocado', 'quantity', 2, 'category', 'Fruit'), named_struct('item', 'Tomato', 'quantity', 5, 'category', 'Vegetable'))),
  (14, map('person14', named_struct('full_name', 'Nina', 'age', 28, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Miami', 'population', 450000), array(named_struct('item', 'Cucumber', 'quantity', 6, 'category', 'Vegetable'), named_struct('item', 'Carrot', 'quantity', 7, 'category', 'Vegetable'))),
  (15, map('person15', named_struct('full_name', 'Emma Smith', 'age', 30, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'New York', 'population', 8000000), array(named_struct('item', 'Banana', 'quantity', 3, 'category', 'Fruit'), named_struct('item', 'Potato', 'quantity', 8, 'category', 'Vegetable')));

-- Add struct_column2
ALTER TABLE hudi_full_schema_change_parquet ADD COLUMNS (
  struct_column2 STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>
);

-- Update existing records (id 0-15) to include struct_column2 as NULL
-- This ensures all records have the same schema structure
INSERT INTO hudi_full_schema_change_parquet VALUES
  (0, map('person0', named_struct('full_name', 'zero', 'age', 2, 'gender', null)), named_struct('country', null, 'city', 'cn', 'population', 1000000), array(named_struct('item', 'Apple', 'quantity', null, 'category', null), named_struct('item', 'Banana', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (1, map('person1', named_struct('full_name', 'Alice', 'age', 25, 'gender', null)), named_struct('country', null, 'city', 'New York', 'population', 8000000), array(named_struct('item', 'Apple', 'quantity', null, 'category', null), named_struct('item', 'Banana', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (2, map('person2', named_struct('full_name', 'Bob', 'age', 30, 'gender', null)), named_struct('country', null, 'city', 'Los Angeles', 'population', 4000000), array(named_struct('item', 'Orange', 'quantity', null, 'category', null), named_struct('item', 'Grape', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (3, map('person3', named_struct('full_name', 'Charlie', 'age', 28, 'gender', null)), named_struct('country', null, 'city', 'Chicago', 'population', 2700000), array(named_struct('item', 'Pear', 'quantity', null, 'category', null), named_struct('item', 'Mango', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (4, map('person4', named_struct('full_name', 'David', 'age', 35, 'gender', null)), named_struct('country', null, 'city', 'Houston', 'population', 2300000), array(named_struct('item', 'Kiwi', 'quantity', null, 'category', null), named_struct('item', 'Pineapple', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (5, map('person5', named_struct('full_name', 'Eve', 'age', 40, 'gender', null)), named_struct('country', 'USA', 'city', 'Phoenix', 'population', 1600000), array(named_struct('item', 'Lemon', 'quantity', null, 'category', null), named_struct('item', 'Lime', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (6, map('person6', named_struct('full_name', 'Frank', 'age', 22, 'gender', null)), named_struct('country', 'USA', 'city', 'Philadelphia', 'population', 1500000), array(named_struct('item', 'Watermelon', 'quantity', null, 'category', null), named_struct('item', 'Strawberry', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (7, map('person7', named_struct('full_name', 'Grace', 'age', 27, 'gender', null)), named_struct('country', 'USA', 'city', 'San Antonio', 'population', 1500000), array(named_struct('item', 'Blueberry', 'quantity', null, 'category', null), named_struct('item', 'Raspberry', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (8, map('person8', named_struct('full_name', 'Hank', 'age', 32, 'gender', null)), named_struct('country', 'USA', 'city', 'San Diego', 'population', 1400000), array(named_struct('item', 'Cherry', 'quantity', 5, 'category', null), named_struct('item', 'Plum', 'quantity', 3, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (9, map('person9', named_struct('full_name', 'Ivy', 'age', 29, 'gender', null)), named_struct('country', 'USA', 'city', 'Dallas', 'population', 1300000), array(named_struct('item', 'Peach', 'quantity', 4, 'category', null), named_struct('item', 'Apricot', 'quantity', 2, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (10, map('person10', named_struct('full_name', 'Jack', 'age', 26, 'gender', null)), named_struct('country', 'USA', 'city', 'Austin', 'population', 950000), array(named_struct('item', 'Fig', 'quantity', 6, 'category', null), named_struct('item', 'Date', 'quantity', 7, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (11, map('person11', named_struct('full_name', 'Karen', 'age', 31, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Seattle', 'population', 750000), array(named_struct('item', 'Coconut', 'quantity', 1, 'category', null), named_struct('item', 'Papaya', 'quantity', 2, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (12, map('person12', named_struct('full_name', 'Leo', 'age', 24, 'gender', 'Male')), named_struct('country', 'USA', 'city', 'Portland', 'population', 650000), array(named_struct('item', 'Guava', 'quantity', 3, 'category', null), named_struct('item', 'Lychee', 'quantity', 4, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (13, map('person13', named_struct('full_name', 'Mona', 'age', 33, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Denver', 'population', 700000), array(named_struct('item', 'Avocado', 'quantity', 2, 'category', 'Fruit'), named_struct('item', 'Tomato', 'quantity', 5, 'category', 'Vegetable')), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (14, map('person14', named_struct('full_name', 'Nina', 'age', 28, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Miami', 'population', 450000), array(named_struct('item', 'Cucumber', 'quantity', 6, 'category', 'Vegetable'), named_struct('item', 'Carrot', 'quantity', 7, 'category', 'Vegetable')), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (15, map('person15', named_struct('full_name', 'Emma Smith', 'age', 30, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'New York', 'population', 8000000), array(named_struct('item', 'Banana', 'quantity', 3, 'category', 'Fruit'), named_struct('item', 'Potato', 'quantity', 8, 'category', 'Vegetable')), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>));

-- Create hudi_full_schema_change_orc table with initial schema (only id)
CREATE TABLE IF NOT EXISTS hudi_full_schema_change_orc (
    id INT
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.base.file.format = 'orc',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/hudi_full_schema_change_orc';

-- Insert initial data (only id)
INSERT INTO hudi_full_schema_change_orc (id) VALUES
  (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15), (16), (17), (18), (19), (20), (21);

-- Execute schema changes: Add complex type columns step by step
ALTER TABLE hudi_full_schema_change_orc ADD COLUMNS (
  new_map_column MAP<STRING, STRUCT<full_name: STRING, age: INT, gender: STRING>>,
  struct_column STRUCT<country: STRING, city: STRING, population: INT>,
  array_column ARRAY<STRUCT<item: STRING, quantity: INT, category: STRING>>
);

-- Insert data with new columns
INSERT INTO hudi_full_schema_change_orc VALUES
  (0, map('person0', named_struct('full_name', 'zero', 'age', 2, 'gender', null)), named_struct('country', null, 'city', 'cn', 'population', 1000000), array(named_struct('item', 'Apple', 'quantity', null, 'category', null), named_struct('item', 'Banana', 'quantity', null, 'category', null))),
  (1, map('person1', named_struct('full_name', 'Alice', 'age', 25, 'gender', null)), named_struct('country', null, 'city', 'New York', 'population', 8000000), array(named_struct('item', 'Apple', 'quantity', null, 'category', null), named_struct('item', 'Banana', 'quantity', null, 'category', null))),
  (2, map('person2', named_struct('full_name', 'Bob', 'age', 30, 'gender', null)), named_struct('country', null, 'city', 'Los Angeles', 'population', 4000000), array(named_struct('item', 'Orange', 'quantity', null, 'category', null), named_struct('item', 'Grape', 'quantity', null, 'category', null))),
  (3, map('person3', named_struct('full_name', 'Charlie', 'age', 28, 'gender', null)), named_struct('country', null, 'city', 'Chicago', 'population', 2700000), array(named_struct('item', 'Pear', 'quantity', null, 'category', null), named_struct('item', 'Mango', 'quantity', null, 'category', null))),
  (4, map('person4', named_struct('full_name', 'David', 'age', 35, 'gender', null)), named_struct('country', null, 'city', 'Houston', 'population', 2300000), array(named_struct('item', 'Kiwi', 'quantity', null, 'category', null), named_struct('item', 'Pineapple', 'quantity', null, 'category', null))),
  (5, map('person5', named_struct('full_name', 'Eve', 'age', 40, 'gender', null)), named_struct('country', 'USA', 'city', 'Phoenix', 'population', 1600000), array(named_struct('item', 'Lemon', 'quantity', null, 'category', null), named_struct('item', 'Lime', 'quantity', null, 'category', null))),
  (6, map('person6', named_struct('full_name', 'Frank', 'age', 22, 'gender', null)), named_struct('country', 'USA', 'city', 'Philadelphia', 'population', 1500000), array(named_struct('item', 'Watermelon', 'quantity', null, 'category', null), named_struct('item', 'Strawberry', 'quantity', null, 'category', null))),
  (7, map('person7', named_struct('full_name', 'Grace', 'age', 27, 'gender', null)), named_struct('country', 'USA', 'city', 'San Antonio', 'population', 1500000), array(named_struct('item', 'Blueberry', 'quantity', null, 'category', null), named_struct('item', 'Raspberry', 'quantity', null, 'category', null))),
  (8, map('person8', named_struct('full_name', 'Hank', 'age', 32, 'gender', null)), named_struct('country', 'USA', 'city', 'San Diego', 'population', 1400000), array(named_struct('item', 'Cherry', 'quantity', 5, 'category', null), named_struct('item', 'Plum', 'quantity', 3, 'category', null))),
  (9, map('person9', named_struct('full_name', 'Ivy', 'age', 29, 'gender', null)), named_struct('country', 'USA', 'city', 'Dallas', 'population', 1300000), array(named_struct('item', 'Peach', 'quantity', 4, 'category', null), named_struct('item', 'Apricot', 'quantity', 2, 'category', null))),
  (10, map('person10', named_struct('full_name', 'Jack', 'age', 26, 'gender', null)), named_struct('country', 'USA', 'city', 'Austin', 'population', 950000), array(named_struct('item', 'Fig', 'quantity', 6, 'category', null), named_struct('item', 'Date', 'quantity', 7, 'category', null))),
  (11, map('person11', named_struct('full_name', 'Karen', 'age', 31, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Seattle', 'population', 750000), array(named_struct('item', 'Coconut', 'quantity', 1, 'category', null), named_struct('item', 'Papaya', 'quantity', 2, 'category', null))),
  (12, map('person12', named_struct('full_name', 'Leo', 'age', 24, 'gender', 'Male')), named_struct('country', 'USA', 'city', 'Portland', 'population', 650000), array(named_struct('item', 'Guava', 'quantity', 3, 'category', null), named_struct('item', 'Lychee', 'quantity', 4, 'category', null))),
  (13, map('person13', named_struct('full_name', 'Mona', 'age', 33, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Denver', 'population', 700000), array(named_struct('item', 'Avocado', 'quantity', 2, 'category', 'Fruit'), named_struct('item', 'Tomato', 'quantity', 5, 'category', 'Vegetable'))),
  (14, map('person14', named_struct('full_name', 'Nina', 'age', 28, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Miami', 'population', 450000), array(named_struct('item', 'Cucumber', 'quantity', 6, 'category', 'Vegetable'), named_struct('item', 'Carrot', 'quantity', 7, 'category', 'Vegetable'))),
  (15, map('person15', named_struct('full_name', 'Emma Smith', 'age', 30, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'New York', 'population', 8000000), array(named_struct('item', 'Banana', 'quantity', 3, 'category', 'Fruit'), named_struct('item', 'Potato', 'quantity', 8, 'category', 'Vegetable')));

-- Add struct_column2
ALTER TABLE hudi_full_schema_change_orc ADD COLUMNS (
  struct_column2 STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>
);

-- Update existing records (id 0-15) to include struct_column2 as NULL
-- This ensures all records have the same schema structure
INSERT INTO hudi_full_schema_change_orc VALUES
  (0, map('person0', named_struct('full_name', 'zero', 'age', 2, 'gender', null)), named_struct('country', null, 'city', 'cn', 'population', 1000000), array(named_struct('item', 'Apple', 'quantity', null, 'category', null), named_struct('item', 'Banana', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (1, map('person1', named_struct('full_name', 'Alice', 'age', 25, 'gender', null)), named_struct('country', null, 'city', 'New York', 'population', 8000000), array(named_struct('item', 'Apple', 'quantity', null, 'category', null), named_struct('item', 'Banana', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (2, map('person2', named_struct('full_name', 'Bob', 'age', 30, 'gender', null)), named_struct('country', null, 'city', 'Los Angeles', 'population', 4000000), array(named_struct('item', 'Orange', 'quantity', null, 'category', null), named_struct('item', 'Grape', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (3, map('person3', named_struct('full_name', 'Charlie', 'age', 28, 'gender', null)), named_struct('country', null, 'city', 'Chicago', 'population', 2700000), array(named_struct('item', 'Pear', 'quantity', null, 'category', null), named_struct('item', 'Mango', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (4, map('person4', named_struct('full_name', 'David', 'age', 35, 'gender', null)), named_struct('country', null, 'city', 'Houston', 'population', 2300000), array(named_struct('item', 'Kiwi', 'quantity', null, 'category', null), named_struct('item', 'Pineapple', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (5, map('person5', named_struct('full_name', 'Eve', 'age', 40, 'gender', null)), named_struct('country', 'USA', 'city', 'Phoenix', 'population', 1600000), array(named_struct('item', 'Lemon', 'quantity', null, 'category', null), named_struct('item', 'Lime', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (6, map('person6', named_struct('full_name', 'Frank', 'age', 22, 'gender', null)), named_struct('country', 'USA', 'city', 'Philadelphia', 'population', 1500000), array(named_struct('item', 'Watermelon', 'quantity', null, 'category', null), named_struct('item', 'Strawberry', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (7, map('person7', named_struct('full_name', 'Grace', 'age', 27, 'gender', null)), named_struct('country', 'USA', 'city', 'San Antonio', 'population', 1500000), array(named_struct('item', 'Blueberry', 'quantity', null, 'category', null), named_struct('item', 'Raspberry', 'quantity', null, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (8, map('person8', named_struct('full_name', 'Hank', 'age', 32, 'gender', null)), named_struct('country', 'USA', 'city', 'San Diego', 'population', 1400000), array(named_struct('item', 'Cherry', 'quantity', 5, 'category', null), named_struct('item', 'Plum', 'quantity', 3, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (9, map('person9', named_struct('full_name', 'Ivy', 'age', 29, 'gender', null)), named_struct('country', 'USA', 'city', 'Dallas', 'population', 1300000), array(named_struct('item', 'Peach', 'quantity', 4, 'category', null), named_struct('item', 'Apricot', 'quantity', 2, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (10, map('person10', named_struct('full_name', 'Jack', 'age', 26, 'gender', null)), named_struct('country', 'USA', 'city', 'Austin', 'population', 950000), array(named_struct('item', 'Fig', 'quantity', 6, 'category', null), named_struct('item', 'Date', 'quantity', 7, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (11, map('person11', named_struct('full_name', 'Karen', 'age', 31, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Seattle', 'population', 750000), array(named_struct('item', 'Coconut', 'quantity', 1, 'category', null), named_struct('item', 'Papaya', 'quantity', 2, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (12, map('person12', named_struct('full_name', 'Leo', 'age', 24, 'gender', 'Male')), named_struct('country', 'USA', 'city', 'Portland', 'population', 650000), array(named_struct('item', 'Guava', 'quantity', 3, 'category', null), named_struct('item', 'Lychee', 'quantity', 4, 'category', null)), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (13, map('person13', named_struct('full_name', 'Mona', 'age', 33, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Denver', 'population', 700000), array(named_struct('item', 'Avocado', 'quantity', 2, 'category', 'Fruit'), named_struct('item', 'Tomato', 'quantity', 5, 'category', 'Vegetable')), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (14, map('person14', named_struct('full_name', 'Nina', 'age', 28, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Miami', 'population', 450000), array(named_struct('item', 'Cucumber', 'quantity', 6, 'category', 'Vegetable'), named_struct('item', 'Carrot', 'quantity', 7, 'category', 'Vegetable')), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>)),
  (15, map('person15', named_struct('full_name', 'Emma Smith', 'age', 30, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'New York', 'population', 8000000), array(named_struct('item', 'Banana', 'quantity', 3, 'category', 'Fruit'), named_struct('item', 'Potato', 'quantity', 8, 'category', 'Vegetable')), CAST(NULL AS STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>));

