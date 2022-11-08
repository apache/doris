LOAD LABEL ${loadLabel} (
    DATA INFILE("s3://${s3BucketName}/regression/ssb/sf100/date.tbl.gz")
    INTO TABLE date
    COLUMNS TERMINATED BY "|"
    (d_datekey,d_date,d_dayofweek,d_month,d_year,d_yearmonthnum,d_yearmonth, d_daynuminweek,d_daynuminmonth,d_daynuminyear,d_monthnuminyear,d_weeknuminyear, d_sellingseason,d_lastdayinweekfl,d_lastdayinmonthfl,d_holidayfl,d_weekdayfl,temp)
)
