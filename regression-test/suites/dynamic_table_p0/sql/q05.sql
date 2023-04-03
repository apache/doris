set enable_vectorized_engine=true;
set exec_mem_limit=8G;
SELECT count() from gharchive;
SELECT `repo.name`, count() AS stars FROM gharchive WHERE `type` = 'WatchEvent' GROUP BY `repo.name` ORDER BY stars DESC, `repo.name` LIMIT 5;
SELECT max(cast(`id` as bigint)) FROM gharchive;
SELECT sum(cast(`id` as bigint)) FROM gharchive;
SELECT sum(cast(`payload.member.id` as bigint)) FROM gharchive;
SELECT sum(cast(`payload.pull_request.milestone.creator.site_admin` as bigint)) FROM gharchive;
SELECT sum(length(`payload.pull_request.base.repo.html_url`)) FROM gharchive;