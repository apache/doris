-- database: presto; groups: qe, horology_functions
SELECT /*+ SET_VAR(enable_nereids_planner=false) */  date '2012-08-08' + interval '2' day,
        timestamp '2012-08-08 01:00' + interval '29' hour,
        timestamp '2012-10-31 01:00' + interval '1' month,
        date '2012-08-08' - interval '2' day,
        timestamp '2012-08-08 01:00' - interval '29' hour,
        timestamp '2012-10-31 01:00' - interval '1' month
