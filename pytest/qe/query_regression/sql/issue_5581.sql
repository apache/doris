--https://github.com/apache/incubator-doris/pull/5581
select bitmap_min(bitmap_from_string('')) value;
select bitmap_min(bitmap_from_string('1,9999999999')) value;
