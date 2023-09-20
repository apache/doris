-- https://github.com/apache/incubator-doris/issues/4975
select locate('', 'abc', 10)
select locate('bar', 'foobarbar')
select locate('bar','foobarbar',5)
