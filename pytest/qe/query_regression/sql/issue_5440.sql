-- https://github.com/apache/incubator-doris/issues/5440
select bitmap_to_string(bitmap_not(bitmap_from_string('1'), bitmap_from_string('2,1'))); 
