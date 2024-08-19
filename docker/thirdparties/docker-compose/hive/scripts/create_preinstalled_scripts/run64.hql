use default;

create table simulation_hive1_orc(
  `a`  boolean,                                     
  `b`  int,                                    
  `c`  string 
)stored as orc
LOCATION '/user/doris/preinstalled_data/orc_table/simulation_hive1_orc';
msck repair table simulation_hive1_orc;

create table test_hive_rename_column_parquet(
  `new_a`  boolean,                                     
  `new_b`  int,                                    
  `c`  string,                                     
  `new_d`  int,                                         
  `f`  string        
)stored as parquet
LOCATION '/user/doris/preinstalled_data/parquet_table/test_hive_rename_column_parquet';
msck repair table test_hive_rename_column_parquet;

create table test_hive_rename_column_orc(
  `new_a`  boolean,                                     
  `new_b`  int,                                    
  `c`  string,                                     
  `new_d`  int,                                         
  `f`  string        
)stored as orc
LOCATION '/user/doris/preinstalled_data/orc_table/test_hive_rename_column_orc';
msck repair table test_hive_rename_column_orc;
