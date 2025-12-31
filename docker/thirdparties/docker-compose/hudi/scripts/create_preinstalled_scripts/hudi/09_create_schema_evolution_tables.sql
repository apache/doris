-- Create schema evolution test tables
-- These tables are used to test adding and altering columns (simple and complex types)
-- Hudi schema evolution works by writing data with different schemas, not via ALTER TABLE
-- The table definition in Hive Metastore reflects the final merged schema
USE regression_hudi;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS adding_simple_columns_table;
DROP TABLE IF EXISTS altering_simple_columns_table;
DROP TABLE IF EXISTS adding_complex_columns_table;
DROP TABLE IF EXISTS altering_complex_columns_table;

-- ============================================================================
-- 1. adding_simple_columns_table - Testing adding simple type columns
-- ============================================================================
-- Step 1: Create table with initial schema (id, name)
CREATE TABLE IF NOT EXISTS adding_simple_columns_table (
    id STRING,
    name STRING
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

-- Insert initial data with schema: id, name
INSERT INTO adding_simple_columns_table (id, name) VALUES
  ('1', 'Alice'),
  ('2', 'Bob'),
  ('3', 'Cathy');

-- Step 2: Insert data with evolved schema (id, name, age)
-- Hudi will automatically evolve the schema to include the new 'age' column
INSERT INTO adding_simple_columns_table (id, name, age) VALUES
  ('4', 'David', 25),
  ('5', 'Eva', 30),
  ('6', 'Frank', 28);

-- Step 3: Update Hive Metastore table definition to reflect final schema
-- This is done by recreating the table with the final schema
DROP TABLE IF EXISTS adding_simple_columns_table;

CREATE TABLE IF NOT EXISTS adding_simple_columns_table (
    id STRING,
    name STRING,
    age INT
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

-- ============================================================================
-- 2. altering_simple_columns_table - Testing altering simple type columns
-- ============================================================================
-- Step 1: Create table with initial schema (id, name, age INT)
CREATE TABLE IF NOT EXISTS altering_simple_columns_table (
    id STRING,
    name STRING,
    age INT
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

-- Insert initial data with schema: id, name, age (INT)
INSERT INTO altering_simple_columns_table (id, name, age) VALUES
  ('1', 'Alice', 25),
  ('2', 'Bob', 30),
  ('3', 'Cathy', 28);

-- Step 2: Insert data with evolved schema (id, name, age DOUBLE)
-- Hudi will automatically evolve the schema to change age from INT to DOUBLE
INSERT INTO altering_simple_columns_table (id, name, age) VALUES
  ('4', 'David', 26.0),
  ('5', 'Eva', 31.5),
  ('6', 'Frank', 29.2);

-- Step 3: Update Hive Metastore table definition to reflect final schema
DROP TABLE IF EXISTS altering_simple_columns_table;

CREATE TABLE IF NOT EXISTS altering_simple_columns_table (
    id STRING,
    name STRING,
    age DOUBLE
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

-- ============================================================================
-- 3. adding_complex_columns_table - Testing adding complex type columns
-- ============================================================================
-- Step 1: Create table with initial schema (id, name, info STRUCT<age, address>)
CREATE TABLE IF NOT EXISTS adding_complex_columns_table (
    id STRING,
    name STRING,
    info STRUCT<age: INT, address: STRING>
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

-- Insert initial data with schema: id, name, info STRUCT<age, address>
INSERT INTO adding_complex_columns_table VALUES
  ('1', 'Alice', named_struct('age', 25, 'address', 'Guangzhou')),
  ('2', 'Bob', named_struct('age', 30, 'address', 'Shanghai')),
  ('3', 'Cathy', named_struct('age', 28, 'address', 'Beijing'));

-- Step 2: Insert data with evolved schema (id, name, info STRUCT<age, address, email>)
-- Hudi will automatically evolve the schema to add 'email' field to the struct
INSERT INTO adding_complex_columns_table VALUES
  ('4', 'David', named_struct('age', 25, 'address', 'Shenzhen', 'email', 'david@example.com')),
  ('5', 'Eva', named_struct('age', 30, 'address', 'Chengdu', 'email', 'eva@example.com')),
  ('6', 'Frank', named_struct('age', 28, 'address', 'Wuhan', 'email', 'frank@example.com'));

-- Step 3: Update Hive Metastore table definition to reflect final schema
DROP TABLE IF EXISTS adding_complex_columns_table;

CREATE TABLE IF NOT EXISTS adding_complex_columns_table (
    id STRING,
    name STRING,
    info STRUCT<age: INT, address: STRING, email: STRING>
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

-- ============================================================================
-- 4. altering_complex_columns_table - Testing altering complex type columns
-- ============================================================================
-- Step 1: Create table with initial schema (id, name, info STRUCT<age INT, address>)
CREATE TABLE IF NOT EXISTS altering_complex_columns_table (
    id STRING,
    name STRING,
    info STRUCT<age: INT, address: STRING>
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

-- Insert initial data with schema: id, name, info STRUCT<age INT, address>
INSERT INTO altering_complex_columns_table VALUES
  ('1', 'Alice', named_struct('age', 25, 'address', 'Guangzhou')),
  ('2', 'Bob', named_struct('age', 30, 'address', 'Shanghai')),
  ('3', 'Cathy', named_struct('age', 28, 'address', 'Beijing'));

-- Step 2: Insert data with evolved schema (id, name, info STRUCT<age DOUBLE, address>)
-- Hudi will automatically evolve the schema to change age from INT to DOUBLE in the struct
INSERT INTO altering_complex_columns_table VALUES
  ('4', 'David', named_struct('age', 26.0, 'address', 'Shenzhen')),
  ('5', 'Eva', named_struct('age', 31.5, 'address', 'Chengdu')),
  ('6', 'Frank', named_struct('age', 29.2, 'address', 'Wuhan'));

-- Step 3: Update Hive Metastore table definition to reflect final schema
DROP TABLE IF EXISTS altering_complex_columns_table;

CREATE TABLE IF NOT EXISTS altering_complex_columns_table (
    id STRING,
    name STRING,
    info STRUCT<age: DOUBLE, address: STRING>
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
