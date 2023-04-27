SELECT
    coalesce(
        coalesce(BITMAP_EMPTY(), BITMAP_EMPTY()),
        CASE
            WHEN ref_12.`cs_bill_cdemo_sk` IS NOT NULL THEN NULL
            ELSE NULL
        END
    ) AS c0
FROM
    regression_test_datev2_tpcds_sf1_p1.catalog_sales AS ref_12
WHERE
    BITMAP_SUBSET_IN_RANGE(
        cast(NULL AS bitmap),
        cast(
            BITMAP_MIN(cast(BITMAP_EMPTY() AS bitmap)) AS bigint
        ),
        cast(ref_12.`cs_bill_addr_sk` AS bigint)
    ) IS NOT NULL
ORDER BY
    ref_12.`cs_sold_date_sk`
LIMIT
    97 OFFSET 125;
