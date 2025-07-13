-- TABLES: var_nested_load_conflict
insert into var_nested_load_conflict values (1, '{"nested": [{"a": 1, "c": 1.1}, {"b": "1"}]}');
insert into var_nested_load_conflict values (2, '{"nested": {"a": 2.5, "b": "123.1"}}');

select v['nested']['a'] from var_nested_load_conflict order by k;
select v['nested']['b'] from var_nested_load_conflict order by k;
select v['nested']['c'] from var_nested_load_conflict order by k;
select v['nested'] from var_nested_load_conflict order by k;

select cast(v['nested']['a'] as array<int>), size(cast(v['nested']['a'] as array<int>)) from var_nested_load_conflict order by k;
select cast(v['nested']['b'] as array<int>), size(cast(v['nested']['b'] as array<int>)) from var_nested_load_conflict order by k;
select cast(v['nested']['c'] as array<int>), size(cast(v['nested']['c'] as array<int>)) from var_nested_load_conflict order by k;

select cast(v['nested']['a'] as array<string>), size(cast(v['nested']['a'] as array<string>)) from var_nested_load_conflict order by k;
select cast(v['nested']['b'] as array<string>), size(cast(v['nested']['b'] as array<string>)) from var_nested_load_conflict order by k;
select cast(v['nested']['c'] as array<string>), size(cast(v['nested']['c'] as array<string>)) from var_nested_load_conflict order by k;