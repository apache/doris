alter table store_sales add constraint ss_c_fk foreign key(ss_customer_sk) references customer(c_customer_sk);
alter table web_sales add constraint ws_c_fk foreign key(ws_bill_customer_sk) references customer(c_customer_sk);
alter table catalog_sales add constraint cs_c_fk foreign key(cs_bill_customer_sk) references customer(c_customer_sk);
