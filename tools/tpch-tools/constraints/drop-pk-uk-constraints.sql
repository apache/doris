-- Drop all TPC-H Primary Key and Unique Key Constraints
alter table region drop r_pk;
alter table nation drop n_pk;
alter table supplier drop s_pk;
alter table customer drop c_pk;
alter table part drop p_pk;
alter table partsupp drop ps_pk;
alter table orders drop o_pk;
alter table lineitem drop l_pk;
alter table region drop r_uk;
alter table nation drop n_uk;
