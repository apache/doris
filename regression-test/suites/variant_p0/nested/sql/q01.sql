-- TABLES: var_nested_load_conflict
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