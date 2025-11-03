insert into quantile_state_basic_agg values
(1,to_quantile_state(-1, 2048)),
(2,to_quantile_state(0, 2048)),(2,to_quantile_state(1, 2048)),
(3,to_quantile_state(0, 2048)),(3,to_quantile_state(1, 2048)),(3,to_quantile_state(2, 2048));
