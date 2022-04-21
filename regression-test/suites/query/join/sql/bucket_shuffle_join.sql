select * from test_bucket_shuffle_join where rectime="2021-12-01 00:00:00" and id in (select k1 from test_join where k1 in (1,2))
