-- https://github.com/apache/incubator-doris/issues/3494
select bitmap_to_string(bitmap_from_string('0, 1, 2'))
select bitmap_to_string(null)
select bitmap_to_string(bitmap_empty())
select bitmap_to_string(to_bitmap(1))
select bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2)))
