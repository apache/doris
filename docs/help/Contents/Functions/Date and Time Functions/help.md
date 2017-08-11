# unix_timestamp
## description
Syntax:
UNIX_TIMESTAMP(), UNIX_TIMESTAMP(date)

将Date或者Datetime类型转化为unix时间戳
如果没有参数，则是将当前的时间转化为时间戳
参数需要是Date或者Datetime类型

## example
mysql> SELECT UNIX_TIMESTAMP();
        -> 1196440210
mysql> SELECT UNIX_TIMESTAMP('2007-11-30 10:30:19');
        -> 1196418619

# from_unixtime
## description
Syntax:
    FROM_UNIXTIME(int unix_timestamp[, string string_format])
    
将unix时间戳转化位对应的time格式，返回的格式由string_format指定
默认为yyyy-MM-dd HH:mm:ss
传入的是整形，返回的是字符串类型
目前string_format只支持两种类型的格式：yyyy-MM-dd，yyyy-MM-dd HH:mm:ss
其余string_format格式是非法的，返回NULL

## example
mysql> SELECT FROM_UNIXTIME(1196440219);
        -> '2007-12-01 00:30:19'

mysql> SELECT FROM_UNIXTIME(1196440219, 'yyyy-MM-dd');
        -> '2007-12-01'

mysql> SELECT FROM_UNIXTIME(1196440219, 'yyyy-MM-dd HH:mm:ss');
        -> '2007-12-01 00:30:19'

# year
## description
Syntax:
YEAR(date)

返回date类型的year部分，范围从1000-9999
参数为Date或者Datetime类型
## example
mysql> SELECT YEAR('1987-01-01');
        -> 1987

# month
## description
Syntax:
MONTH(date)

返回时间类型中的月份信息，范围是1, 12
参数为Date或者Datetime类型

## example
mysql> SELECT MONTH('1987-01-02');
        -> 01

# day
## description
Syntax:
DAY(date)

与DAYOFMONTH是同义词，请`help dayofmonth`

# dayofmonth
## description
Syntax:
DAYOFMONTH(date)

获得日期中的天信息，返回值范围从1-31。
需要传入date类型

## example
mysql> SELECT DAYOFMONTH('1987-01-02');
        -> 2

# dayofyear
## description
Syntax:
DAYOFYEAR(date)

获得日期中对应当年中的哪一天。
输入值为date类型

## example
mysql> SELECT DAYOFYEAR('2007-02-03');
        -> 34

# weekofyear
## description
Syntax:
WEEKOFYEAR(date)

获得一年中的第几周
输入值为date类型

## example
mysql> SELECT WEEKOFYEAR('2008-02-20');
        -> 8

# hour
## description
Syntax:
HOUR(date)

获得时间中对应的小时信息
这里Palo跟MySQL不太一样，因为MySQL是支持Time类型的
Palo没有Time类型，所以输入的内容是Date或者Datetime。

## example
mysql> select hour("2000-01-02 12:34:56");
        -> 12

# minute
## description
Syntax:
MINUTE(date)

获得日期中的分钟信息
这里Palo跟MySQL不太一样，因为MySQL是支持Time类型的
Palo没有Time类型，所以输入的内容是Date或者Datetime。

## example
mysql> SELECT MINUTE("2000-01-02 12:34:56");
        -> 34

# second
## description
Syntax:
SECOND(date)

获得时间中的秒信息
这里Palo跟MySQL不太一样，因为MySQL是支持Time类型的
Palo没有Time类型，所以输入的内容是Date或者Datetime。
## example
mysql> SELECT SECOND("2000-01-02 12:34:56");
        -> 56

# now
## description
Syntax:
NOW()

获得当前的时间，以Datetime类型返回

## example
mysql> SELECT NOW();
        -> '2007-12-15 23:50:26'

# current_timestamp
## description
Syntax:
CURRENT_TIMESTAMP()

与NOW()是同义词

# datediff
## description
Syntax:
DATEDIFF(expr1,expr2)

计算expr1 - expr2，结果精确到天。
要求传入的两个值需要是datetime类型。

## example
mysql> SELECT DATEDIFF(CAST ('2007-12-31 23:59:59' AS DATETIME),CAST ('2007-12-30' AS DATETIME));
        -> 1
mysql> SELECT DATEDIFF(CAST('2010-11-30 23:59:59' AS DATETIME), CAST('2010-12-31' AS DATETIME));
        -> -31

# date_add
## description
Syntax:
DATE_ADD(date,INTERVAL expr unit)

对时间类型进行加法运算
支持的time unit包括
YEAR, MONTH, DAY, HOUR, MINUTE, SECOND

## example
mysql>  DATE_ADD(date,INTERVAL expr unit)
        -> 1987-01-02 00:00:00

# date_sub
## description
Syntax:
DATE_SUB(date,INTERVAL expr unit)

与DATE_ADD相反，对时间类型进行减法运算
支持的time unit包括
YEAR, MONTH, DAY, HOUR, MINUTE, SECOND

## example
mysql>  DATE_SUB(date,INTERVAL expr unit)
        -> 1986-12-31 00:00:00

# date_format
## description
Syntax:
DATE_FORMAT(date, format)

将日期类型按照format的类型转化位字符串，
当前支持最大128字节的字符串，如果返回值长度超过128，则返回NULL
format的含义如下：
%a  Abbreviated weekday name (Sun..Sat)
%b  Abbreviated month name (Jan..Dec)
%c  Month, numeric (0..12)
%D  Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
%d  Day of the month, numeric (00..31)
%e  Day of the month, numeric (0..31)
%f  Microseconds (000000..999999)
%H  Hour (00..23)
%h  Hour (01..12)
%I  Hour (01..12)
%i  Minutes, numeric (00..59)
%j  Day of year (001..366)
%k  Hour (0..23)
%l  Hour (1..12)
%M  Month name (January..December)
%m  Month, numeric (00..12)
%p  AM or PM
%r  Time, 12-hour (hh:mm:ss followed by AM or PM)
%S  Seconds (00..59)
%s  Seconds (00..59)
%T  Time, 24-hour (hh:mm:ss)
%U  Week (00..53), where Sunday is the first day of the week; WEEK() mode 0
%u  Week (00..53), where Monday is the first day of the week; WEEK() mode 1
%V  Week (01..53), where Sunday is the first day of the week; WEEK() mode 2; used with %X
%v  Week (01..53), where Monday is the first day of the week; WEEK() mode 3; used with %x
%W  Weekday name (Sunday..Saturday)
%w  Day of the week (0=Sunday..6=Saturday)
%X  Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
%x  Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
%Y  Year, numeric, four digits
%y  Year, numeric (two digits)
%%  A literal “%” character
%x  x, for any “x” not listed above

## example
mysql> SELECT DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y');
        -> 'Sunday October 2009'
mysql> SELECT DATE_FORMAT('2007-10-04 22:23:00', '%H:%i:%s');
        -> '22:23:00'
mysql> SELECT DATE_FORMAT('1900-10-04 22:23:00', '%D %y %a %d %m %b %j');
        -> '4th 00 Thu 04 10 Oct 277'
mysql> SELECT DATE_FORMAT('1997-10-04 22:23:00', '%H %k %I %r %T %S %w');
        -> '22 22 10 10:23:00 PM 22:23:00 00 6'
mysql> SELECT DATE_FORMAT('1999-01-01', '%X %V'); 
        -> '1998 52'
mysql> SELECT DATE_FORMAT('2006-06-01', '%d');
        -> '01'

# from_days
## description
Syntax:
FROM_DAYS(N)
通过距离0000-01-01日的天数计算出哪一天

## example
mysql> SELECT FROM_DAYS(730669);
        -> '2007-07-03'

# to_days
## description
Syntax:
TO_DAYS(date)
返回date距离0000-01-01的天数

## example
mysql> SELECT TO_DAYS(950501);
        -> 728779
mysql> SELECT TO_DAYS('2007-10-07');
        -> 733321

# str_to_date
## description
Syntax:
STR_TO_DATE(str, format)
通过format指定的方式将str转化为DATE类型，如果转化结果不对返回NULL
支持的format格式与date_format一致

## example
mysql> SELECT STR_TO_DATE('2014-12-21 12:34:56', '%Y-%m-%d %H:%i:%s');
        -> 2014-12-21 12:34:56
mysql> SELECT STR_TO_DATE('200442 Monday', '%X%V %W');
        -> 2004-10-18

# monthname
## description
Syntax:
MONTHNAME(DATE)

返回日期对应的月份名字

## example
mysql> SELECT MONTHNAME('2008-02-03');
        -> 'February'

# monthname
## description
Syntax:
MONTHNAME(DATE)

返回日期对应的日期名字

## example
mysql> SELECT DAYNAME('2007-02-03');
        -> 'Saturday'
