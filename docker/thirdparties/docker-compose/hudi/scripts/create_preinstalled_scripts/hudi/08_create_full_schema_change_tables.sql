-- Create full schema change test tables with complex types (map, struct, array)
-- These tables are used to test complex schema evolution scenarios
-- Note: The tables are created with the final schema (after evolution) to match test expectations
USE regression_hudi;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS hudi_full_schema_change_parquet;
DROP TABLE IF EXISTS hudi_full_schema_change_orc;

-- Create hudi_full_schema_change_parquet table with final schema
-- The test expects: id, new_map_column, struct_column, array_column, struct_column2
CREATE TABLE IF NOT EXISTS hudi_full_schema_change_parquet (
    id INT,
    new_map_column MAP<STRING, STRUCT<full_name: STRING, age: INT, gender: STRING>>,
    struct_column STRUCT<country: STRING, city: STRING, population: INT>,
    array_column ARRAY<STRUCT<item: STRING, quantity: INT, category: STRING>>,
    struct_column2 STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>
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

-- Create hudi_full_schema_change_orc table with final schema
CREATE TABLE IF NOT EXISTS hudi_full_schema_change_orc (
    id INT,
    new_map_column MAP<STRING, STRUCT<full_name: STRING, age: INT, gender: STRING>>,
    struct_column STRUCT<country: STRING, city: STRING, population: INT>,
    array_column ARRAY<STRUCT<item: STRING, quantity: INT, category: STRING>>,
    struct_column2 STRUCT<b: STRUCT<cc: STRING, new_dd: INT>, new_a: STRUCT<new_aa: INT, bb: STRING>, c: INT>
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

-- Insert data matching the expected test results
-- Data structure matches the test case expectations from p2 test comments
INSERT INTO hudi_full_schema_change_parquet VALUES
  (0, map('person0', named_struct('full_name', 'zero', 'age', 2, 'gender', null)), named_struct('country', null, 'city', 'cn', 'population', 1000000), array(named_struct('item', 'Apple', 'quantity', null, 'category', null), named_struct('item', 'Banana', 'quantity', null, 'category', null)), null),
  (1, map('person1', named_struct('full_name', 'Alice', 'age', 25, 'gender', null)), named_struct('country', null, 'city', 'New York', 'population', 8000000), array(named_struct('item', 'Apple', 'quantity', null, 'category', null), named_struct('item', 'Banana', 'quantity', null, 'category', null)), null),
  (2, map('person2', named_struct('full_name', 'Bob', 'age', 30, 'gender', null)), named_struct('country', null, 'city', 'Los Angeles', 'population', 4000000), array(named_struct('item', 'Orange', 'quantity', null, 'category', null), named_struct('item', 'Grape', 'quantity', null, 'category', null)), null),
  (3, map('person3', named_struct('full_name', 'Charlie', 'age', 28, 'gender', null)), named_struct('country', null, 'city', 'Chicago', 'population', 2700000), array(named_struct('item', 'Pear', 'quantity', null, 'category', null), named_struct('item', 'Mango', 'quantity', null, 'category', null)), null),
  (4, map('person4', named_struct('full_name', 'David', 'age', 35, 'gender', null)), named_struct('country', null, 'city', 'Houston', 'population', 2300000), array(named_struct('item', 'Kiwi', 'quantity', null, 'category', null), named_struct('item', 'Pineapple', 'quantity', null, 'category', null)), null),
  (5, map('person5', named_struct('full_name', 'Eve', 'age', 40, 'gender', null)), named_struct('country', 'USA', 'city', 'Phoenix', 'population', 1600000), array(named_struct('item', 'Lemon', 'quantity', null, 'category', null), named_struct('item', 'Lime', 'quantity', null, 'category', null)), null),
  (6, map('person6', named_struct('full_name', 'Frank', 'age', 22, 'gender', null)), named_struct('country', 'USA', 'city', 'Philadelphia', 'population', 1500000), array(named_struct('item', 'Watermelon', 'quantity', null, 'category', null), named_struct('item', 'Strawberry', 'quantity', null, 'category', null)), null),
  (7, map('person7', named_struct('full_name', 'Grace', 'age', 27, 'gender', null)), named_struct('country', 'USA', 'city', 'San Antonio', 'population', 1500000), array(named_struct('item', 'Blueberry', 'quantity', null, 'category', null), named_struct('item', 'Raspberry', 'quantity', null, 'category', null)), null),
  (8, map('person8', named_struct('full_name', 'Hank', 'age', 32, 'gender', null)), named_struct('country', 'USA', 'city', 'San Diego', 'population', 1400000), array(named_struct('item', 'Cherry', 'quantity', 5, 'category', null), named_struct('item', 'Plum', 'quantity', 3, 'category', null)), null),
  (9, map('person9', named_struct('full_name', 'Ivy', 'age', 29, 'gender', null)), named_struct('country', 'USA', 'city', 'Dallas', 'population', 1300000), array(named_struct('item', 'Peach', 'quantity', 4, 'category', null), named_struct('item', 'Apricot', 'quantity', 2, 'category', null)), null),
  (10, map('person10', named_struct('full_name', 'Jack', 'age', 26, 'gender', null)), named_struct('country', 'USA', 'city', 'Austin', 'population', 950000), array(named_struct('item', 'Fig', 'quantity', 6, 'category', null), named_struct('item', 'Date', 'quantity', 7, 'category', null)), null),
  (11, map('person11', named_struct('full_name', 'Karen', 'age', 31, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Seattle', 'population', 750000), array(named_struct('item', 'Coconut', 'quantity', 1, 'category', null), named_struct('item', 'Papaya', 'quantity', 2, 'category', null)), null),
  (12, map('person12', named_struct('full_name', 'Leo', 'age', 24, 'gender', 'Male')), named_struct('country', 'USA', 'city', 'Portland', 'population', 650000), array(named_struct('item', 'Guava', 'quantity', 3, 'category', null), named_struct('item', 'Lychee', 'quantity', 4, 'category', null)), null),
  (13, map('person13', named_struct('full_name', 'Mona', 'age', 33, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Denver', 'population', 700000), array(named_struct('item', 'Avocado', 'quantity', 2, 'category', 'Fruit'), named_struct('item', 'Tomato', 'quantity', 5, 'category', 'Vegetable')), null),
  (14, map('person14', named_struct('full_name', 'Nina', 'age', 28, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Miami', 'population', 450000), array(named_struct('item', 'Cucumber', 'quantity', 6, 'category', 'Vegetable'), named_struct('item', 'Carrot', 'quantity', 7, 'category', 'Vegetable')), null),
  (15, map('person15', named_struct('full_name', 'Emma Smith', 'age', 30, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'New York', 'population', 8000000), array(named_struct('item', 'Banana', 'quantity', 3, 'category', 'Fruit'), named_struct('item', 'Potato', 'quantity', 8, 'category', 'Vegetable')), null),
  (16, map('person16', named_struct('full_name', 'Liam Brown', 'age', 28, 'gender', 'Male')), named_struct('country', 'UK', 'city', 'London', 'population', 9000000), array(named_struct('item', 'Bread', 'quantity', 2, 'category', 'Food'), named_struct('item', 'Milk', 'quantity', 1, 'category', 'Dairy')), named_struct('b', named_struct('cc', 'NestedCC', 'new_dd', 75), 'new_a', named_struct('new_aa', 50, 'bb', 'NestedBB'), 'c', 9)),
  (17, map('person17', named_struct('full_name', 'Olivia Davis', 'age', 40, 'gender', 'Female')), named_struct('country', 'Australia', 'city', 'Sydney', 'population', 5000000), array(named_struct('item', 'Orange', 'quantity', 4, 'category', 'Fruit'), named_struct('item', 'Broccoli', 'quantity', 6, 'category', 'Vegetable')), named_struct('b', named_struct('cc', 'UpdatedCC', 'new_dd', 88), 'new_a', named_struct('new_aa', 60, 'bb', 'UpdatedBB'), 'c', 12)),
  (18, map('person18', named_struct('full_name', 'Noah Wilson', 'age', 33, 'gender', 'Male')), named_struct('country', 'Germany', 'city', 'Berlin', 'population', 3700000), array(named_struct('item', 'Cheese', 'quantity', 2, 'category', 'Dairy'), named_struct('item', 'Lettuce', 'quantity', 5, 'category', 'Vegetable')), named_struct('b', named_struct('cc', 'NestedCC18', 'new_dd', 95), 'new_a', named_struct('new_aa', 70, 'bb', 'NestedBB18'), 'c', 15)),
  (19, map('person19', named_struct('full_name', 'Ava Martinez', 'age', 29, 'gender', 'Female')), named_struct('country', 'France', 'city', 'Paris', 'population', 2100000), array(named_struct('item', 'Strawberry', 'quantity', 12, 'category', 'Fruit'), named_struct('item', 'Spinach', 'quantity', 7, 'category', 'Vegetable')), named_struct('b', named_struct('cc', 'ReorderedCC', 'new_dd', 101), 'new_a', named_struct('new_aa', 85, 'bb', 'ReorderedBB'), 'c', 18)),
  (20, map('person20', named_struct('full_name', 'James Lee', 'age', 38, 'gender', 'Male')), named_struct('country', 'Japan', 'city', 'Osaka', 'population', 2700000), array(named_struct('item', 'Mango', 'quantity', 6, 'category', 'Fruit'), named_struct('item', 'Onion', 'quantity', 3, 'category', 'Vegetable')), named_struct('b', named_struct('cc', 'FinalCC', 'new_dd', 110), 'new_a', named_struct('new_aa', 95, 'bb', 'FinalBB'), 'c', 21)),
  (21, map('person21', named_struct('full_name', 'Sophia White', 'age', 45, 'gender', 'Female')), named_struct('country', 'Italy', 'city', 'Rome', 'population', 2800000), array(named_struct('item', 'Pasta', 'quantity', 4, 'category', 'Food'), named_struct('item', 'Olive', 'quantity', 9, 'category', 'Food')), named_struct('b', named_struct('cc', 'ExampleCC', 'new_dd', 120), 'new_a', named_struct('new_aa', 100, 'bb', 'ExampleBB'), 'c', 25));

-- Insert same data into ORC table
INSERT INTO hudi_full_schema_change_orc VALUES
  (0, map('person0', named_struct('full_name', 'zero', 'age', 2, 'gender', null)), named_struct('country', null, 'city', 'cn', 'population', 1000000), array(named_struct('item', 'Apple', 'quantity', null, 'category', null), named_struct('item', 'Banana', 'quantity', null, 'category', null)), null),
  (1, map('person1', named_struct('full_name', 'Alice', 'age', 25, 'gender', null)), named_struct('country', null, 'city', 'New York', 'population', 8000000), array(named_struct('item', 'Apple', 'quantity', null, 'category', null), named_struct('item', 'Banana', 'quantity', null, 'category', null)), null),
  (2, map('person2', named_struct('full_name', 'Bob', 'age', 30, 'gender', null)), named_struct('country', null, 'city', 'Los Angeles', 'population', 4000000), array(named_struct('item', 'Orange', 'quantity', null, 'category', null), named_struct('item', 'Grape', 'quantity', null, 'category', null)), null),
  (3, map('person3', named_struct('full_name', 'Charlie', 'age', 28, 'gender', null)), named_struct('country', null, 'city', 'Chicago', 'population', 2700000), array(named_struct('item', 'Pear', 'quantity', null, 'category', null), named_struct('item', 'Mango', 'quantity', null, 'category', null)), null),
  (4, map('person4', named_struct('full_name', 'David', 'age', 35, 'gender', null)), named_struct('country', null, 'city', 'Houston', 'population', 2300000), array(named_struct('item', 'Kiwi', 'quantity', null, 'category', null), named_struct('item', 'Pineapple', 'quantity', null, 'category', null)), null),
  (5, map('person5', named_struct('full_name', 'Eve', 'age', 40, 'gender', null)), named_struct('country', 'USA', 'city', 'Phoenix', 'population', 1600000), array(named_struct('item', 'Lemon', 'quantity', null, 'category', null), named_struct('item', 'Lime', 'quantity', null, 'category', null)), null),
  (6, map('person6', named_struct('full_name', 'Frank', 'age', 22, 'gender', null)), named_struct('country', 'USA', 'city', 'Philadelphia', 'population', 1500000), array(named_struct('item', 'Watermelon', 'quantity', null, 'category', null), named_struct('item', 'Strawberry', 'quantity', null, 'category', null)), null),
  (7, map('person7', named_struct('full_name', 'Grace', 'age', 27, 'gender', null)), named_struct('country', 'USA', 'city', 'San Antonio', 'population', 1500000), array(named_struct('item', 'Blueberry', 'quantity', null, 'category', null), named_struct('item', 'Raspberry', 'quantity', null, 'category', null)), null),
  (8, map('person8', named_struct('full_name', 'Hank', 'age', 32, 'gender', null)), named_struct('country', 'USA', 'city', 'San Diego', 'population', 1400000), array(named_struct('item', 'Cherry', 'quantity', 5, 'category', null), named_struct('item', 'Plum', 'quantity', 3, 'category', null)), null),
  (9, map('person9', named_struct('full_name', 'Ivy', 'age', 29, 'gender', null)), named_struct('country', 'USA', 'city', 'Dallas', 'population', 1300000), array(named_struct('item', 'Peach', 'quantity', 4, 'category', null), named_struct('item', 'Apricot', 'quantity', 2, 'category', null)), null),
  (10, map('person10', named_struct('full_name', 'Jack', 'age', 26, 'gender', null)), named_struct('country', 'USA', 'city', 'Austin', 'population', 950000), array(named_struct('item', 'Fig', 'quantity', 6, 'category', null), named_struct('item', 'Date', 'quantity', 7, 'category', null)), null),
  (11, map('person11', named_struct('full_name', 'Karen', 'age', 31, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Seattle', 'population', 750000), array(named_struct('item', 'Coconut', 'quantity', 1, 'category', null), named_struct('item', 'Papaya', 'quantity', 2, 'category', null)), null),
  (12, map('person12', named_struct('full_name', 'Leo', 'age', 24, 'gender', 'Male')), named_struct('country', 'USA', 'city', 'Portland', 'population', 650000), array(named_struct('item', 'Guava', 'quantity', 3, 'category', null), named_struct('item', 'Lychee', 'quantity', 4, 'category', null)), null),
  (13, map('person13', named_struct('full_name', 'Mona', 'age', 33, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Denver', 'population', 700000), array(named_struct('item', 'Avocado', 'quantity', 2, 'category', 'Fruit'), named_struct('item', 'Tomato', 'quantity', 5, 'category', 'Vegetable')), null),
  (14, map('person14', named_struct('full_name', 'Nina', 'age', 28, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'Miami', 'population', 450000), array(named_struct('item', 'Cucumber', 'quantity', 6, 'category', 'Vegetable'), named_struct('item', 'Carrot', 'quantity', 7, 'category', 'Vegetable')), null),
  (15, map('person15', named_struct('full_name', 'Emma Smith', 'age', 30, 'gender', 'Female')), named_struct('country', 'USA', 'city', 'New York', 'population', 8000000), array(named_struct('item', 'Banana', 'quantity', 3, 'category', 'Fruit'), named_struct('item', 'Potato', 'quantity', 8, 'category', 'Vegetable')), null),
  (16, map('person16', named_struct('full_name', 'Liam Brown', 'age', 28, 'gender', 'Male')), named_struct('country', 'UK', 'city', 'London', 'population', 9000000), array(named_struct('item', 'Bread', 'quantity', 2, 'category', 'Food'), named_struct('item', 'Milk', 'quantity', 1, 'category', 'Dairy')), named_struct('b', named_struct('cc', 'NestedCC', 'new_dd', 75), 'new_a', named_struct('new_aa', 50, 'bb', 'NestedBB'), 'c', 9)),
  (17, map('person17', named_struct('full_name', 'Olivia Davis', 'age', 40, 'gender', 'Female')), named_struct('country', 'Australia', 'city', 'Sydney', 'population', 5000000), array(named_struct('item', 'Orange', 'quantity', 4, 'category', 'Fruit'), named_struct('item', 'Broccoli', 'quantity', 6, 'category', 'Vegetable')), named_struct('b', named_struct('cc', 'UpdatedCC', 'new_dd', 88), 'new_a', named_struct('new_aa', 60, 'bb', 'UpdatedBB'), 'c', 12)),
  (18, map('person18', named_struct('full_name', 'Noah Wilson', 'age', 33, 'gender', 'Male')), named_struct('country', 'Germany', 'city', 'Berlin', 'population', 3700000), array(named_struct('item', 'Cheese', 'quantity', 2, 'category', 'Dairy'), named_struct('item', 'Lettuce', 'quantity', 5, 'category', 'Vegetable')), named_struct('b', named_struct('cc', 'NestedCC18', 'new_dd', 95), 'new_a', named_struct('new_aa', 70, 'bb', 'NestedBB18'), 'c', 15)),
  (19, map('person19', named_struct('full_name', 'Ava Martinez', 'age', 29, 'gender', 'Female')), named_struct('country', 'France', 'city', 'Paris', 'population', 2100000), array(named_struct('item', 'Strawberry', 'quantity', 12, 'category', 'Fruit'), named_struct('item', 'Spinach', 'quantity', 7, 'category', 'Vegetable')), named_struct('b', named_struct('cc', 'ReorderedCC', 'new_dd', 101), 'new_a', named_struct('new_aa', 85, 'bb', 'ReorderedBB'), 'c', 18)),
  (20, map('person20', named_struct('full_name', 'James Lee', 'age', 38, 'gender', 'Male')), named_struct('country', 'Japan', 'city', 'Osaka', 'population', 2700000), array(named_struct('item', 'Mango', 'quantity', 6, 'category', 'Fruit'), named_struct('item', 'Onion', 'quantity', 3, 'category', 'Vegetable')), named_struct('b', named_struct('cc', 'FinalCC', 'new_dd', 110), 'new_a', named_struct('new_aa', 95, 'bb', 'FinalBB'), 'c', 21)),
  (21, map('person21', named_struct('full_name', 'Sophia White', 'age', 45, 'gender', 'Female')), named_struct('country', 'Italy', 'city', 'Rome', 'population', 2800000), array(named_struct('item', 'Pasta', 'quantity', 4, 'category', 'Food'), named_struct('item', 'Olive', 'quantity', 9, 'category', 'Food')), named_struct('b', named_struct('cc', 'ExampleCC', 'new_dd', 120), 'new_a', named_struct('new_aa', 100, 'bb', 'ExampleBB'), 'c', 25));

