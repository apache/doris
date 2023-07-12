```sql
CREATE
    [DEFINER = user]
    EVENT
    event_name
    ON SCHEDULE schedule
    [COMMENT 'string']
    DO event_body;

schedule: {
    AT timestamp 
  | EVERY interval
    [STARTS timestamp ]
    [ENDS timestamp ]
}

interval:
    quantity { DAY | HOUR | MINUTE |
              WEEK | SECOND }
```

eg:
```sql
CREATE
EVENT e_daily
    ON SCHEDULE
      EVERY 1 DAY 
      STARTS '2011-11-17 23:59:00'
      ENDS '2038-01-19 03:14:07'
    COMMENT 'Saves total number of sessions'
    DO
        INSERT INTO site_activity.totals (time, total)
        SELECT CURRENT_TIMESTAMP, COUNT(*)
        FROM site_activity.sessions;

```