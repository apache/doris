---
{
    "title": "DATE_FORMAT",
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

## date_format
### Description
#### Syntax

`VARCHAR DATE_FORMAT (DATETIME DATE, VARCHAR Format)`


Convert the date type to a string according to the format type.
Convert the date type to a string according to the format type. Currently, it supports a maximum of 128 bytes for the string. If the return value length exceeds 128 bytes, then it returns NULL.

The date parameter is the valid date. Format specifies the date/time output format.

The formats available are:

% a | Abbreviation for Sunday Name

% B | Abbreviated Monthly Name

% C | Month, numerical value

% D | Sky in the Moon with English Prefix

% d | Monthly day, numerical value (00-31)

% e | Monthly day, numerical value (0-31)

% f | microseconds

% H | Hours (00-23)

% h | hour (01-12)

% I | Hours (01-12)

% I | min, numerical value (00-59)

% J | Days of Year (001-366)

% k | hours (0-23)

% L | Hours (1-12)

% M | Moon Name

% m | month, numerical value (00-12)

%p | AM or PM

% R | Time, 12 - hour (hh: mm: SS AM or PM)

% S | seconds (00-59)

% s | seconds (00-59)

% T | Time, 24 - hour (hh: mm: ss)

% U | Week (00-53) Sunday is the first day of the week

% U | Week (00 - 53) Monday is the first day of the week

% V | Week (01-53) Sunday is the first day of the week, and% X is used.

% v | Week (01 - 53) Monday is the first day of the week, and% x is used

% W | Sunday

% w | Weekly day (0 = Sunday, 6 = Saturday)

% X | Year, where Sunday is the first day of the week, 4 places, and% V use

% x | year, of which Monday is the first day of the week, 4 places, and% V

% Y | Year, 4

% y | Year, 2

%%  | Represent %

Also support 3 formats:

yyyyMMdd

yyyy-MM-dd

yyyy-MM-dd HH:mm:ss

### example

```
mysql> select date_format('2009-10-04 22:23:00', '%W %M %Y');
+------------------------------------------------+
| date_format('2009-10-04 22:23:00', '%W %M %Y') |
+------------------------------------------------+
| Sunday October 2009                            |
+------------------------------------------------+

mysql> select date_format('2007-10-04 22:23:00', '%H:%i:%s');
+------------------------------------------------+
| date_format('2007-10-04 22:23:00', '%H:%i:%s') |
+------------------------------------------------+
| 22:23:00                                       |
+------------------------------------------------+

mysql> select date_format('1900-10-04 22:23:00', '%D %y %a %d %m %b %j');
+------------------------------------------------------------+
| date_format('1900-10-04 22:23:00', '%D %y %a %d %m %b %j') |
+------------------------------------------------------------+
| 4th 00 Thu 04 10 Oct 277                                   |
+------------------------------------------------------------+

mysql> select date_format('1997-10-04 22:23:00', '%H %k %I %r %T %S %w');
+------------------------------------------------------------+
| date_format('1997-10-04 22:23:00', '%H %k %I %r %T %S %w') |
+------------------------------------------------------------+
| 22 22 10 10:23:00 PM 22:23:00 00 6                         |
+------------------------------------------------------------+

mysql> select date_format('1999-01-01 00:00:00', '%X %V'); 
+---------------------------------------------+
| date_format('1999-01-01 00:00:00', '%X %V') |
+---------------------------------------------+
| 1998 52                                     |
+---------------------------------------------+

mysql> select date_format('2006-06-01', '%d');
+------------------------------------------+
| date_format('2006-06-01 00:00:00', '%d') |
+------------------------------------------+
| 01                                       |
+------------------------------------------+

mysql> select date_format('2006-06-01', '%%%d');
+--------------------------------------------+
| date_format('2006-06-01 00:00:00', '%%%d') |
+--------------------------------------------+
| %01                                        |
+--------------------------------------------+
```
### keywords
    DATE_FORMAT,DATE,FORMAT
