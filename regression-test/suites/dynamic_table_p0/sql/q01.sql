SELECT count() FROM test_btc_json;
SELECT avg(fee) FROM test_btc_json;
SELECT avg(size(`inputs.prev_out.spending_outpoints.n`))  FROM test_btc_json;
SELECT avg(size(`inputs.prev_out.spending_outpoints.tx_index`))  FROM test_btc_json;
select `inputs.prev_out.spending_outpoints.tx_index`, fee from test_btc_json order by fee;
select `out.tx_index`[-1] from test_btc_json order by  `out.tx_index`[-1];
select `out.tx_index`, fee, `out.value`[1] from test_btc_json where array_contains(`out.value`, 2450939412);