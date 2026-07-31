-- TPC-H Primary Key Constraints
alter table region add constraint r_pk primary key (r_regionkey);
alter table nation add constraint n_pk primary key (n_nationkey);
alter table supplier add constraint s_pk primary key (s_suppkey);
alter table customer add constraint c_pk primary key (c_custkey);
alter table part add constraint p_pk primary key (p_partkey);
alter table partsupp add constraint ps_pk primary key (ps_partkey, ps_suppkey);
alter table orders add constraint o_pk primary key (o_orderkey);
alter table lineitem add constraint l_pk primary key (l_orderkey, l_linenumber);

-- TPC-H Foreign Key Constraints
alter table nation add constraint n_r_fk foreign key (n_regionkey) references region(r_regionkey);
alter table supplier add constraint s_n_fk foreign key (s_nationkey) references nation(n_nationkey);
alter table customer add constraint c_n_fk foreign key (c_nationkey) references nation(n_nationkey);
alter table partsupp add constraint ps_p_fk foreign key (ps_partkey) references part(p_partkey);
alter table partsupp add constraint ps_s_fk foreign key (ps_suppkey) references supplier(s_suppkey);
alter table orders add constraint o_c_fk foreign key (o_custkey) references customer(c_custkey);
alter table lineitem add constraint l_o_fk foreign key (l_orderkey) references orders(o_orderkey);
alter table lineitem add constraint l_p_fk foreign key (l_partkey) references part(p_partkey);
alter table lineitem add constraint l_s_fk foreign key (l_suppkey) references supplier(s_suppkey);
alter table lineitem add constraint l_ps_fk foreign key (l_partkey, l_suppkey) references partsupp(ps_partkey, ps_suppkey);

-- TPC-H Unique Key Constraints
alter table region add constraint r_uk unique (r_name);
alter table nation add constraint n_uk unique (n_name);
