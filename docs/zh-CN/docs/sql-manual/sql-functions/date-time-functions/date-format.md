---
{
    "title": "DATE_FORMAT",
    "language": "zh-CN"
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
### description
#### Syntax

`VARCHAR DATE_FORMAT(DATETIME date, VARCHAR format)`


将日期类型按照format的类型转化为字符串，
当前支持最大128字节的字符串，如果返回值长度超过128字节，则返回NULL。

date 参数是合法的日期。format 规定日期/时间的输出格式。

可以使用的格式有：

%a | 缩写星期名     
                              
%b | 缩写月名   
                                  
%c | 月，数值 

%D | 带有英文前缀的月中的天       
               
%d | 月的天，数值(00-31)

%e | 月的天，数值(0-31)

%f | 微秒

%H | 小时 (00-23)

%h | 小时 (01-12)

%I | 小时 (01-12)

%i | 分钟，数值(00-59)

%j | 年的天 (001-366)

%k | 小时 (0-23)

%l | 小时 (1-12)

%M | 月名

%m | 月，数值(00-12)

%p | AM 或 PM

%r | 时间，12-小时（hh:mm:ss AM 或 PM）

%S | 秒(00-59)

%s | 秒(00-59)

%T | 时间, 24-小时 (hh:mm:ss)

%U | 周 (00-53) 星期日是一周的第一天

%u | 周 (00-53) 星期一是一周的第一天

%V | 周 (01-53) 星期日是一周的第一天，与 %X 使用

%v | 周 (01-53) 星期一是一周的第一天，与 %x 使用

%W | 星期名

%w | 周的天 （0=星期日, 6=星期六）

%X | 年，其中的星期日是周的第一天，4 位，与 %V 使用

%x | 年，其中的星期一是周的第一天，4 位，与 %v 使用

%Y | 年，4 位          
                           
%y | 年，2 位

%% | 用于表示 %

还可以使用三种特殊格式：

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
