use rqg;

-- set eager_aggregation_mode = 1;

SELECT
    CASE
        WHEN '2003-12-21' > tbl_alias1.col_date_undef_signed__index_inverted
        OR NULL THEN FALSE
        ELSE TRUE
    END col_alias1
FROM
    table_2_50_undef_partitions2_keys3_properties4_distributed_by5 AS tbl_alias1
WHERE
    FALSE
    OR NULL
GROUP BY
    col_alias1;
