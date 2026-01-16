
use rqg;
set eager_aggregation_mode=1;
select /*+leading({ss dt} ws)*/  dt.d_year 
        ,min(d_year) brand
        ,sum(d_year) sum_agg
    from  store_sales ss
        join date_dim dt
        join web_sales ws
    where dt.d_date_sk = ss_sold_date_sk
    and ss_item_sk = ws_item_sk
    group by dt.d_year, ss_hdemo_sk + d_moy
    having brand is null;
