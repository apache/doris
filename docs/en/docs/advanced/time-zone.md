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

Doris supports multiple time zone settings

## Noun Interpretation

* FE: Frontend, the front-end node of Doris. Responsible for metadata management and request access.
* BE: Backend, Doris's back-end node. Responsible for query execution and data storage.

## Basic concepts

There are multiple time zone related parameters in Doris

* `system_time_zone`:

When the server starts, it will be set automatically according to the time zone set by the machine, which cannot be modified after setting.

* `time_zone`:

Server current time zone, set it at session level or global level.

## Specific operations

1. `SHOW VARIABLES LIKE '% time_zone%'`

    View the current time zone related configuration

2. `SET time_zone = 'Asia/Shanghai'`

    This command can set the session level time zone, which will fail after disconnection.

3. `SET global time_zone = 'Asia/Shanghai'`
   
    This command can set time zone parameters at the global level. The FE will persist the parameters and will not fail when the connection is disconnected.

### Impact of time zone

Time zone setting affects the display and storage of time zone sensitive values.

It includes the values displayed by time functions such as `NOW()` or `CURTIME()`, as well as the time values in `SHOW LOAD` and `SHOW BACKENDS` statements.

However, it does not affect the `LESS THAN VALUE` of the time-type partition column in the `CREATE TABLE` statement, nor does it affect the display of values stored as `DATE/DATETIME` type.

Functions affected by time zone:

* `FROM_UNIXTIME`: Given a UTC timestamp, return the date and time of the specified time zone, such as `FROM_UNIXTIME(0)`, return the CST time zone: `1970-01-08:00`.

* `UNIX_TIMESTAMP`: Given a specified time zone date and time, return UTC timestamp, such as CST time zone `UNIX_TIMESTAMP('1970-01 08:00:00')`, return `0`.

* `CURTIME`: Returns the datetime of specified time zone.

* `NOW`: Returns the specified date and time of specified time zone.

* `CONVERT_TZ`: Converts a date and time from one specified time zone to another.

## Restrictions

Time zone values can be given in several formats, case-insensitive:

* A string representing UTC offset, such as '+10:00' or '-6:00'.

* Standard time zone formats, such as "Asia/Shanghai", "America/Los_Angeles"

* Abbreviated time zone formats such as MET and CTT are not supported. Because the abbreviated time zone is ambiguous in different scenarios, it is not recommended to use it.

* In order to be compatible with Doris and support CST abbreviated time zone, CST will be internally transferred to "Asia/Shanghai", which is Chinese standard time zone.

## Time zone format list

[List of TZ database time zones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)
