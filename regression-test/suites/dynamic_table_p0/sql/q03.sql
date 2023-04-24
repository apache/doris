set exec_mem_limit=8G;
SELECT `repo.name`, count() AS stars FROM test_ghdata_json  WHERE `type` = 'WatchEvent' GROUP BY `repo.name` ORDER BY stars DESC, `repo.name` LIMIT 5;
SELECT max(size(`payload.pull_request.assignees.type`)) FROM test_ghdata_json;
SELECT max(size(`payload.pull_request.assignees.url`)) FROM test_ghdata_json;
SELECT max(cast(`id` as bigint)) FROM test_ghdata_json;
SELECT sum(cast(`id` as bigint)) FROM test_ghdata_json;

