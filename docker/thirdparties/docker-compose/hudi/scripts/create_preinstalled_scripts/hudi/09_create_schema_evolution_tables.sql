-- Create schema evolution test tables
-- These tables are used to test adding and altering columns (simple and complex types)
-- Note: Tables are created with final schema (after evolution) to match test expectations
USE regression_hudi;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS adding_simple_columns_table;
DROP TABLE IF EXISTS altering_simple_columns_table;
DROP TABLE IF EXISTS adding_complex_columns_table;
DROP TABLE IF EXISTS altering_complex_columns_table;

-- Create adding_simple_columns_table - for testing adding simple type columns
-- Final schema after evolution: id, name, age, city (age and city were added)
CREATE TABLE IF NOT EXISTS adding_simple_columns_table (
    id INT,
    name STRING,
    age INT,
    city STRING
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/adding_simple_columns_table';

-- Insert data with evolved schema
INSERT INTO adding_simple_columns_table VALUES
  (1, 'Alice', 25, 'New York'),
  (2, 'Bob', 30, 'Los Angeles'),
  (3, 'Charlie', 28, 'Chicago'),
  (4, 'David', 35, 'Houston'),
  (5, 'Eve', 22, 'Phoenix');

-- Create altering_simple_columns_table - for testing altering simple type columns
-- Final schema after evolution: id, name, age (age type may have been altered)
CREATE TABLE IF NOT EXISTS altering_simple_columns_table (
    id INT,
    name STRING,
    age BIGINT
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/altering_simple_columns_table';

-- Insert data with evolved schema
INSERT INTO altering_simple_columns_table VALUES
  (1, 'Alice', 25),
  (2, 'Bob', 30),
  (3, 'Charlie', 28),
  (4, 'David', 35),
  (5, 'Eve', 22);

-- Create adding_complex_columns_table - for testing adding complex type columns
-- Final schema after evolution: id, name, address (address was added)
CREATE TABLE IF NOT EXISTS adding_complex_columns_table (
    id INT,
    name STRING,
    address STRUCT<street: STRING, city: STRING, zipcode: STRING>
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/adding_complex_columns_table';

-- Insert data with evolved schema
INSERT INTO adding_complex_columns_table VALUES
  (1, 'Alice', named_struct('street', '123 Main St', 'city', 'New York', 'zipcode', '10001')),
  (2, 'Bob', named_struct('street', '456 Oak Ave', 'city', 'Los Angeles', 'zipcode', '90001')),
  (3, 'Charlie', named_struct('street', '789 Pine Rd', 'city', 'Chicago', 'zipcode', '60601')),
  (4, 'David', named_struct('street', '321 Elm St', 'city', 'Houston', 'zipcode', '77001')),
  (5, 'Eve', named_struct('street', '654 Maple Dr', 'city', 'Phoenix', 'zipcode', '85001'));

-- Create altering_complex_columns_table - for testing altering complex type columns
-- Final schema after evolution: id, name, address (address structure was altered to include zipcode)
CREATE TABLE IF NOT EXISTS altering_complex_columns_table (
    id INT,
    name STRING,
    address STRUCT<street: STRING, city: STRING, zipcode: STRING>
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id',
  hoodie.schema.on.read.enable = 'true',
  hoodie.metadata.enable = 'false',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/altering_complex_columns_table';

-- Insert data with evolved schema
INSERT INTO altering_complex_columns_table VALUES
  (1, 'Alice', named_struct('street', '123 Main St', 'city', 'New York', 'zipcode', '10001')),
  (2, 'Bob', named_struct('street', '456 Oak Ave', 'city', 'Los Angeles', 'zipcode', '90001')),
  (3, 'Charlie', named_struct('street', '789 Pine Rd', 'city', 'Chicago', 'zipcode', '60601')),
  (4, 'David', named_struct('street', '321 Elm St', 'city', 'Houston', 'zipcode', '77001')),
  (5, 'Eve', named_struct('street', '654 Maple Dr', 'city', 'Phoenix', 'zipcode', '85001'));

