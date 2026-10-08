create database if not exists demo.test_db;
use demo.test_db;

-- Fixtures for float/double predicate pushdown.
--
-- Iceberg keeps NaN out of a column's bounds entirely (spec: "NaNs are not permitted as lower or upper
-- bounds" -- it is recorded only in nan_value_counts) and orders -0.0 strictly before +0.0, while Doris
-- matches rows with "NaN is greater than everything" and IEEE zero equality (-0.0 == 0.0). A predicate
-- translated literally therefore prunes files that do hold matching rows.
--
-- COALESCE(1) keeps every table at exactly one data file, so a wrongly pruned file shows up directly as
-- inputSplitNum=0 in EXPLAIN. These must be written by spark, not by Doris: Doris reports no
-- nan_value_counts at all, and its parquet writer normalizes zero bounds, so Doris-written files do not
-- carry the metadata shape that triggers the pruning.

drop table if exists float_prune_nan_only;
create table float_prune_nan_only (id int, d double) using iceberg;
insert into float_prune_nan_only
select /*+ COALESCE(1) */ * from values (7, cast('NaN' as double)) as v(id, d);

-- NaN hides outside the bounds, so this file is pruned by the bounds alone for `d > 5`.
drop table if exists float_prune_nan_mixed;
create table float_prune_nan_mixed (id int, d double) using iceberg;
insert into float_prune_nan_mixed
select /*+ COALESCE(1) */ * from values (1, cast(1.0 as double)), (2, cast('NaN' as double)) as v(id, d);

drop table if exists float_prune_nan_mixed_float;
create table float_prune_nan_mixed_float (id int, f float) using iceberg;
insert into float_prune_nan_mixed_float
select /*+ COALESCE(1) */ * from values (1, cast(1.0 as float)), (2, cast('NaN' as float)) as v(id, f);

-- Iceberg keeps the sign in the bounds (lower = upper = -0.0), so a `d = 0` / `d >= 0` bound placed at +0.0
-- sorts strictly after it and prunes the file.
drop table if exists float_prune_negzero;
create table float_prune_negzero (id int, d double) using iceberg;
insert into float_prune_negzero
select /*+ COALESCE(1) */ * from values (7, cast('-0.0' as double)) as v(id, d);
