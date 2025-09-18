use demo.test_db;
CREATE TABLE tmp_schema_change_branch (id bigint, data string, col float);
INSERT INTO tmp_schema_change_branch VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', 3.0);
ALTER TABLE tmp_schema_change_branch CREATE BRANCH test_branch;
ALTER TABLE tmp_schema_change_branch CREATE TAG test_tag;
ALTER TABLE tmp_schema_change_branch DROP COLUMN col;
ALTER TABLE tmp_schema_change_branch ADD COLUMN new_col date;
INSERT INTO tmp_schema_change_branch VALUES (4, 'd', date('2024-04-04')), (5, 'e', date('2024-05-05'));
