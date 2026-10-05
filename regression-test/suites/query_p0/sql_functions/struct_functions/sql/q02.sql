use regression_test_query_p0_sql_functions_struct_functions;

DROP TABLE IF EXISTS `test_nested_type_compatibility`;

-- Create a simple table to test nested type compatibility
CREATE TABLE IF NOT EXISTS `test_nested_type_compatibility` (
  `id` int,
  -- Test cases for ARRAY types
  `arr_date` array<date>,
  `arr_datetime` array<datetime>,
  `arr_decimal` array<decimal(10,2)>,
  `arr_nested_date` array<array<date>>,
  
  -- Test cases for MAP types
  `map_string_date` map<string, date>,
  `map_datetime_decimal` map<datetime, decimal(10,2)>,
  `map_nested_date` map<string, array<date>>,
  
  -- Test cases for STRUCT types
  `struct_mixed` struct<name:string, birth_date:date, salary:decimal(10,2)>,
  `struct_map` struct<col: map<date, datetime(6)>, col1: map<string, date>>
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);

-- Insert test data
INSERT INTO `test_nested_type_compatibility` VALUES
(1, 
 -- ARRAY data
 ['2023-01-01', '2023-12-31'], 
 ['2023-01-01 10:00:00', '2023-12-31 23:59:59'], 
 [123.45, 678.90], 
 [['2023-01-01'], ['2023-12-31']],
 
 -- MAP data
 {'user1': '2023-01-01', 'user2': '2023-12-31'}, 
 {'2023-01-01 10:00:00': 123.45, '2023-12-31 23:59:59': 678.90}, 
 {'user1': ['2023-01-01', '2023-12-31'], 'user2': ['2023-06-15']},
 
 -- STRUCT data
 struct('Charlie', '1985-05-15', 50000.00), 
 struct(map('2023-01-01', '2023-01-01 10:00:00.123456', '2023-12-31', '2023-12-31 23:59:59.999999'), map('user1', '2023-01-01', 'user2', '2023-12-31'))
);

-- Test ARRAY constructors with date types - should trigger compatibility check
select array('2023-01-01', '2023-12-31') from test_nested_type_compatibility where id = 1;
select array(arr_date) from test_nested_type_compatibility where id = 1;
select array('2023-01-01 10:00:00', '2023-12-31 23:59:59') from test_nested_type_compatibility where id = 1;
select array(arr_datetime) from test_nested_type_compatibility where id = 1;
select array(123.45, 678.90) from test_nested_type_compatibility where id = 1;
select array(arr_decimal) from test_nested_type_compatibility where id = 1;
select array(array('2023-01-01'), array('2023-12-31')) from test_nested_type_compatibility where id = 1;
select array(arr_nested_date) from test_nested_type_compatibility where id = 1;

-- Test MAP constructors with date types - should trigger compatibility check
select map('user1', '2023-01-01', 'user2', '2023-12-31') from test_nested_type_compatibility where id = 1;
select map('first', map_string_date['user1'], 'second', map_string_date['user2']) from test_nested_type_compatibility where id = 1;
select map('literal', '2023-01-01', 'from_table', map_string_date['user1']) from test_nested_type_compatibility where id = 1;
select map('2023-01-01 10:00:00', 123.45, '2023-12-31 23:59:59', 678.90) from test_nested_type_compatibility where id = 1;
select map('first', map_datetime_decimal['2023-01-01 10:00:00'], 'second', map_datetime_decimal['2023-12-31 23:59:59']) from test_nested_type_compatibility where id = 1;
select map('user1', array('2023-01-01', '2023-12-31'), 'user2', array('2023-06-15')) from test_nested_type_compatibility where id = 1;
select map('first', map_nested_date['user1'], 'second', map_nested_date['user2']) from test_nested_type_compatibility where id = 1;

-- Test STRUCT constructors with date types - should trigger compatibility check
select struct('Alice', '1990-01-01', 60000.00) from test_nested_type_compatibility where id = 1;
select struct(struct_mixed) from test_nested_type_compatibility where id = 1;
select struct('outer', struct_mixed, 'literal', struct('David', '1988-03-20', 80000.00)) from test_nested_type_compatibility where id = 1;
select struct(map('2023-01-01', '2023-01-01 10:00:00.123456', '2023-12-31', '2023-12-31 23:59:59.999999'), map('user1', '2023-01-01', 'user2', '2023-12-31')) from test_nested_type_compatibility where id = 1;
select struct(map_string_date, map_datetime_decimal) from test_nested_type_compatibility where id = 1;
