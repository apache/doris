-- https://github.com/apache/incubator-doris/issues/5346
select bitmap_to_string(bitmap_not(bitmap_from_string("1,2,3,4"), bitmap_from_string("3,4,5,6")));
