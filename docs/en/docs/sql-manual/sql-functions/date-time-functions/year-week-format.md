---
{
    "title": "YEAR_WEEK_FORMAT",
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

## year_week_format
### description
#### Syntax

`VARCHAR YEAR_WEEK_FORMAT(DATETIME date, 
[INT first_day_of_week, INT first_week_of_year_policy, VARCHAR format])`

Calculates the year and week number of a date and converts it to a string of the format type.

The date param is a valid date. 
The last three params are described as follows, using the default values if not specified (first_day_of_week=0, first_week_of_year_policy=0, format='%YWK%W') 
Currently, strings of up to 16 bytes are supported, and NULL is returned if the return value is longer than 16 bytes.

1. first_day_of_week:The starting week of the week, where 0 is Monday, 6 is Sunday, and so on.
2. first_week_of_year_policy:How to determine the first week of the year, there are three strategies.
strategies 0：The week of January 1 is the first week of the year.
strategies 1：After January 1, the week in which the first week begins is the first week of the year.
strategies 2：The first week longer than four days is the first week of the year.
3. format: Specifies the output format for the date/datetime.

format The following formats are available:

%W | numbering of weeks, double-digit.

%w | numbering of weeks, minimum one-digit, maximum double-digit.

%Y | year, four figures.       
                           
%y | year, Last two digits.

%% | express %

### example

```
mysql> select year_week_format(20240101);
+----------------------------+
| year_week_format(20240101) |
+----------------------------+
| 2024WK01                   |
+----------------------------+

mysql> select year_week_format(20240101,0,0,'%YWK%W');
+--------------------------------------------+
| year_week_format(20240101, 0, 0, '%YWK%W') |
+--------------------------------------------+
| 2024WK01                                   |
+--------------------------------------------+

mysql> select year_week_format(20240101,1,1,'%YWK%W');
+--------------------------------------------+
| year_week_format(20240101, 1, 1, '%YWK%W') |
+--------------------------------------------+
| 2023WK52                                   |
+--------------------------------------------+

mysql> select year_week_format(20240101,3,2,'%YWK%W');
+--------------------------------------------+
| year_week_format(20240101, 3, 2, '%YWK%W') |
+--------------------------------------------+
| 2023WK53                                   |
+--------------------------------------------+

```

### keywords

    YEAR_WEEK_FORMAT,DATE,FORMAT
