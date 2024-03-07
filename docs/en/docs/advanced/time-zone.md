---
{
    "title": "Time Zone",
    "language": "en"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Time Zone

Doris supports custom time zone settings

## Basic concepts

The following two time zone related parameters exist within Doris:

- `system_time_zone` : When the server starts up, it will be set automatically according to the time zone set by the machine, and cannot be modified after it is set.
- `time_zone` : The current time zone of the cluster.

## Specific operations

1. `SHOW VARIABLES LIKE '% time_zone%'`

    View the current time zone related configuration

2. `SET [global] time_zone = 'Asia/Shanghai'`

   This command sets the time zone at the session level. If the `global` keyword is used, Doris FE persists the parameter and it takes effect for all new sessions afterwards.

## Data source

The time zone data contains the name of the time zone, the corresponding time offset, and the change of daylight saving time. On the machine where the BE is located, the sources of the data are as follows:

1. the directory returned by command `TZDIR`. If was not supported, the directory `/usr/share/zoneinfo`.
2. the `zoneinfo` directory generated under the Doris BE deployment directory. The `resource/zoneinfo.tar.gz` directory from the Doris Repository.

Look up the above data sources in order and use the current item if found. If the BE configuration item `use_doris_tzfile` is true, the search for the first item is skipped. If neither is found, the Doris BE will fail to start, please rebuild the BE correctly or get the distribution.

## Impact of time zone

### 1. functions

Includes values displayed by time functions such as `NOW()` or `CURTIME()`, and also time values in `show load`, `show backends`.

However, it does not affect the less than value of the time-type partitioned columns in `create table`, nor does it affect the display of values stored as `date/datetime` types.

Functions affected by time zone:

* `FROM_UNIXTIME`: Given a UTC timestamp, return the date and time of the specified time zone, such as `FROM_UNIXTIME(0)`, return the CST time zone: `1970-01-08:00`.

* `UNIX_TIMESTAMP`: Given a specified time zone date and time, return UTC timestamp, such as CST time zone `UNIX_TIMESTAMP('1970-01 08:00:00')`, return `0`.

* `CURTIME`: Returns the datetime of specified time zone.

* `NOW`: Returns the specified date and time of specified time zone.

* `CONVERT_TZ`: Converts a date and time from one specified time zone to another.

### 2. Values of time types

For `DATE`, `DATEV2`, `DATETIME`, `DATETIMEV2` types, we support time zone conversion when inserting data.

- If the data comes with a time zone, such as "2020-12-12 12:12:12+08:00", and the current Doris `time_zone = +00:00`, you get the actual value "2020-12-12 04:12:12".
- If the data does not come with a time zone, such as "2020-12-12 12:12:12", the time is considered to be absolute and no conversion occurs.

### 3. Daylight Saving Time

Daylight Saving Time is essentially the actual time offset of a named time zone, which changes on certain dates.

For example, the `America/Los_Angeles` time zone contains a Daylight Saving Time adjustment that begins and ends approximately in March and November of each year. That is, the `America/Los_Angeles` actual time zone offset changes from `-08:00` to `-07:00` at the start of Daylight Savings Time in March, and from `-07:00` to `-08:00` at the end of Daylight Savings Time in November.
If you do not want Daylight Saving Time to be turned on, set `time_zone` to `-08:00` instead of `America/Los_Angeles`.

## Usage

Time zone values can be given in a variety of formats. The following standard formats are well supported in Doris:

1. standard named time zone formats, such as "Asia/Shanghai", "America/Los_Angeles".
2. standard offset formats, such as "+02:30", "-10:00".
3. abbreviated time zone formats, currently only support:
   1. "GMT", "UTC", equivalent to "+00:00" time zone
   2. "CST", which is equivalent to the "Asia/Shanghai" time zone
4. single letter Z, for Zulu time zone, equivalent to "+00:00" time zone

Note: Some other formats are currently supported in some imports in Doris due to different implementations. **Production environments should not rely on these formats that are not listed here, and their behaviour may change at any time**, so keep an eye on the relevant changelog for version updates.

## Best Practices

### Time Zone Sensitive Data

The time zone issue involves three main influences:

1. session variable `time_zone` -- cluster timezone
2. header `timezone` specified during import(Stream Load, Broker Load etc.) -- importing timezone
3. timezone type literal "+08:00" in "2023-12-12 08:00:00+08:00" -- data timezone

We can understand it as follows:

Doris is currently compatible with importing data into Doris under all time zones. Since time types such as `DATETIME` do not contain time zone information, the time type data in the Doris cluster can be divided into two categories:

1. absolute time
2. time in a specific time zone

Absolute time means that it is associated with a data scenario that is independent of the time zone. For this type of data, it should be imported without any time zone suffix and they will be stored as is. For this type of time, since it is not associated with an actual time zone, taking the result of a function such as `unix_timestamp` is meaningless. Changes to the cluster `time_zone` will not affect its use.

The time in a particular time zone. This "specific time zone" is our session variable `time_zone`. As a matter of best practice, this variable should be set before data is imported **and never changed**. At this point in time, this type of time data in the Doris cluster will actually mean: time in the `time_zone` time zone. Example:

```sql
mysql> select @@time_zone;
+----------------+
| @@time_zone    |
+----------------+
| Asia/Hong_Kong |
+----------------+
1 row in set (0.12 sec)

mysql> insert into dtv23 values('2020-12-12 12:12:12+02:00'); --- absolute timezone is +02:00
Query OK, 1 row affected (0.27 sec)

mysql> select * from dtv23;
+-------------------------+
| k0                      |
+-------------------------+
| 2020-12-12 18:12:12.000 | --- converted to Doris' cluster timezone Asia/Hong_Kong. This semantics should be maintained. 
+-------------------------+
1 row in set (0.19 sec)

mysql> set time_zone = 'America/Los_Angeles';
Query OK, 0 rows affected (0.15 sec)

mysql> select * from dtv23;
+-------------------------+
| k0                      |
+-------------------------+
| 2020-12-12 18:12:12.000 | --- If time_zone is modified, the time value does not change and its meaning is disturbed.
+-------------------------+
1 row in set (0.18 sec)

mysql> insert into dtv23 values('2020-12-12 12:12:12+02:00');
Query OK, 1 row affected (0.17 sec)

mysql> select * from dtv23;
+-------------------------+
| k0                      |
+-------------------------+
| 2020-12-12 02:12:12.000 |
| 2020-12-12 18:12:12.000 |
+-------------------------+ --- the data has been misplaced.
2 rows in set (0.19 sec)
```

In summary, the best practice for dealing with time zone issues is to:

1. Confirm the timezone characterised by the cluster and set the `time_zone` before use, and do not change it afterwards.
2. Set the header `timezone` on import to match the cluster `time_zone`.
3. For absolute time, import without a time zone suffix; for time in a time zone, import with a specific time zone suffix, which will be converted to the Doris `time_zone` time zone after import.

### Daylight Saving Time

The start and end times for Daylight Saving Time are taken from the [current time zone data source](#data-source) and may not necessarily correspond exactly to the actual officially recognised times for the current year's time zone location. This data is maintained by ICANN. If you need to ensure that Daylight Saving Time behaves as specified for the current year, please make sure that data source selected by Doris is the latest ICANN published time zone data, which could be downloaded at [Extended Reading](#extended-reading).

## Extended Reading

- [List of tz database time zones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)
- [IANA Time Zone Database](https://www.iana.org/time-zones)
- [The tz-announce Archives](https://mm.icann.org/pipermail/tz-announce/)
