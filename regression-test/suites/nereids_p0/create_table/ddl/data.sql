insert into test_all_types values
    (1, true, 3, 4, 5, 6, 7, 0.1, 0.001, 0.32, 'a', 'ab', 'abc', '2023-02-13', '2023-02-13 00:24:42', '2023-02-13', '2023-02-13 01:43:24', 421.24, 431.42, 4435.23455, [1, 2], [[1, 3], [2]]),
    (2, true, 4, 5, 5, 6, 7, 0.1, 0.001, 0.32, 'b', 'ab', 'abcd', '2023-02-13', '2023-02-13 00:24:42', '2023-02-13', '2023-02-13 01:43:24', 421.24, 431.42, 4435.23455, [2, 2], [[1, 2], [4]]),
    (3, true, 5, 6, 5, 6, 7, 0.1, 0.001, 0.32, 'c', 'ab', 'abcde', '2023-02-13', '2023-02-13 00:24:42', '2023-02-13', '2023-02-13 01:43:24', 421.24, 431.42, 4435.23455, [1, 3], [[1, 3], [5]]);
    
insert into test_agg_key values
    (1, true, 2, 3, 4, 5, 6),
    (5, false, 3, 4, 5, 6, 7),
    (5, true, 3, 5, 7, 9, 11),
    (13, true, 4, 5, 6, 7, 8),
    (13, true, 8, 10, 12, 14, 16);
    
insert into test_uni_key values
    (1, true, 2, 3, 4, 5, 6),
    (null, null, 1, 2, 3, 4, 5),
    (5, false, 3, 4, 5, 6, 7),
    (5, true, 3, 5, 7, 9, 11),
    (13, true, 4, 5, 6, 7, 8),
    (13, true, 8, 10, 12, 14, 16);
    
insert into test_uni_key_mow values
    (1, true, 2, 3, 4, 5, 6),
    (null, null, 1, 2, 3, 4, 5),
    (5, false, 3, 4, 5, 6, 7),
    (5, true, 3, 5, 7, 9, 11),
    (13, true, 4, 5, 6, 7, 8),
    (13, true, 8, 10, 12, 14, 16);
    
insert into test_not_null values
    (1, true, 2, 3, 4, 5, 6),
    (5, false, 3, 4, 5, 6, 7),
    (13, true, 4, 5, 6, 7, 8);
    
insert into test_random values
    (1, true, 2, 3, 4, 5, 6),
    (5, false, 3, 4, 5, 6, 7),
    (13, true, 4, 5, 6, 7, 8);
    
insert into test_random_auto values
    (1, true, 2, 3, 4, 5, 6),
    (5, false, 3, 4, 5, 6, 7),
    (13, true, 4, 5, 6, 7, 8);
    
insert into test_less_than_partition values
    (1, true, 2, 3, 4, 5, 6),
    (5, false, 3, 4, 5, 6, 7),
    (13, true, 4, 5, 6, 7, 8);
  
insert into test_range_partition values
    (1, true, 2, 3, 4, 5, 6),
    (5, false, 3, 4, 5, 6, 7),
    (13, true, 4, 5, 6, 7, 8);
    
insert into test_step_partition values
    (1, true, 2, 3, 4, 5, 6),
    (5, false, 3, 4, 5, 6, 7),
    (13, true, 4, 5, 6, 7, 8);
    
insert into test_date_step_partition values
    ('2020-12-21', 1, 'a'),
    ('2021-12-21', 2, 'ab'),
    ('2022-12-21', 3, 'abc');
    
insert into test_list_partition values
    (1, true, 2, 3, 4, 5, 6),
    (5, false, 3, 4, 5, 6, 7),
    (13, true, 4, 5, 6, 7, 8);
    
insert into test_rollup values
    (1, true, 2, 3, 4, 5, 6),
    (5, false, 3, 4, 5, 6, 7),
    (13, true, 4, 5, 6, 7, 8);

insert into test_default_value values
    (1, true, 2, '2023-02-13'),
    (5, false, 3, '2023-02-13'),
    (13, true, 4, '2023-02-13');