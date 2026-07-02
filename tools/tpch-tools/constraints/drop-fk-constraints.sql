-- Drop all TPC-H Foreign Key Constraints
alter table nation drop constraint n_r_fk;
alter table supplier drop constraint s_n_fk;
alter table customer drop constraint c_n_fk;
alter table partsupp drop constraint ps_p_fk;
alter table partsupp drop constraint ps_s_fk;
alter table orders drop constraint o_c_fk;
alter table lineitem drop constraint l_o_fk;
alter table lineitem drop constraint l_p_fk;
alter table lineitem drop constraint l_s_fk;
alter table lineitem drop constraint l_ps_fk;
