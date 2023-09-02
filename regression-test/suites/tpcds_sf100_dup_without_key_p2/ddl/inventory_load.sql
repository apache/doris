LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/tpcds/sf100/inventory.dat.gz")
    INTO TABLE inventory
    COLUMNS TERMINATED BY "|"
    (inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand)
)