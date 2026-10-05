-- https://github.com/apache/incubator-doris/issues/3275
select str_to_date('11/09/2011', '%m/%d/%Y');
select str_to_date('2017-01-06 10:20:30','%Y-%m-%d %H:%i:%s')
select str_to_date('2017-01-06 10:20:30','%Y-%m-%d')
select str_to_date('20140422154706','%Y%m%d%H%i%s')
