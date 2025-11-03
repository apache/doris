-- database: presto; groups: no_from
SET enable_nereids_planner = TRUE;
SELECT 1,
       'a',
       COUNT(),
       SUM(1) + 1,
       AVG(2) / COUNT(),
       MAX(3),
       MIN(4),
       RANK() OVER() AS w_rank,
        DENSE_RANK() OVER() AS w_dense_rank,
        ROW_NUMBER() OVER() AS w_row_number,
        SUM(5) OVER() AS w_sum,
        AVG(6) OVER() AS w_avg,
        COUNT() OVER() AS w_count,
        MAX(7) OVER() AS w_max,
        MIN(8) OVER() AS w_min;