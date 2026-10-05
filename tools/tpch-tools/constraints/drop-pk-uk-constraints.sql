-- drop constraint all TPC-H Primary Key and Unique Key Constraints
alter table region drop constraint r_pk;
alter table nation drop constraint n_pk;
alter table supplier drop constraint s_pk;
alter table customer drop constraint c_pk;
alter table part drop constraint p_pk;
alter table partsupp drop constraint ps_pk;
alter table orders drop constraint o_pk;
alter table lineitem drop constraint l_pk;
alter table region drop constraint r_uk;
alter table nation drop constraint n_uk;
