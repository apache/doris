INSERT OVERWRITE TABLE auto_inc_target PARTITION (*)
SELECT '2099-12-01' AS close_account_month,
    ROW_NUMBER() OVER () AS auto_increment_id,
    0 AS close_account_status,
    `_id`,
    main.`transaction_id` AS transaction_id,
    CURRENT_TIMESTAMP() AS insert_time
FROM (
        SELECT `_id`,
            `transaction_id`
        FROM auto_inc_src
        WHERE dt = '2025-12-22'
    ) main
    LEFT JOIN (
        SELECT DISTINCT transaction_id
        FROM auto_inc_target
        WHERE close_account_status = 1
    ) et ON main.transaction_id = et.transaction_id
WHERE et.transaction_id IS NULL