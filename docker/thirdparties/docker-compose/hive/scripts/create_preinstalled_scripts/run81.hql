use `default`;

SET hive.merge.mapfiles=false;
SET hive.merge.mapredfiles=false;



CREATE TABLE test_topn_rf_null_parquet (
  id INT,
  value INT,
  name STRING
)
STORED AS PARQUET;

INSERT INTO test_topn_rf_null_parquet VALUES (1, 100, 'Alice');
INSERT INTO test_topn_rf_null_parquet VALUES (2, 200, null);
INSERT INTO test_topn_rf_null_parquet VALUES (3, 300, 'Charlie');
INSERT INTO test_topn_rf_null_parquet VALUES (4, 400, 'David');
INSERT INTO test_topn_rf_null_parquet VALUES (5, null, null);
INSERT INTO test_topn_rf_null_parquet VALUES (6, 600, 'Frank');
INSERT INTO test_topn_rf_null_parquet VALUES (7, null, 'Grace');
INSERT INTO test_topn_rf_null_parquet VALUES (8, 800, null);
INSERT INTO test_topn_rf_null_parquet VALUES (9, null, 'Ivan');
INSERT INTO test_topn_rf_null_parquet VALUES (10, 1000, 'Judy');



CREATE TABLE test_topn_rf_null_orc (
  id INT,
  value INT,
  name STRING
)
STORED AS ORC;

INSERT INTO test_topn_rf_null_orc VALUES (1, 100, 'Alice');
INSERT INTO test_topn_rf_null_orc VALUES (2, 200, null);
INSERT INTO test_topn_rf_null_orc VALUES (3, 300, 'Charlie');
INSERT INTO test_topn_rf_null_orc VALUES (4, 400, 'David');
INSERT INTO test_topn_rf_null_orc VALUES (5, null, null);
INSERT INTO test_topn_rf_null_orc VALUES (6, 600, 'Frank');
INSERT INTO test_topn_rf_null_orc VALUES (7, null, 'Grace');
INSERT INTO test_topn_rf_null_orc VALUES (8, 800, null);
INSERT INTO test_topn_rf_null_orc VALUES (9, null, 'Ivan');
INSERT INTO test_topn_rf_null_orc VALUES (10, 1000, 'Judy');




