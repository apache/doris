ALTER SYSTEM ADD BACKEND '127.0.0.1:9050';

drop catalog if exists lakesoul;

create catalog `lakesoul`  properties (
	'type'='lakesoul',
	'lakesoul.pg.username'='lakesoul_test',
	'lakesoul.pg.password'='lakesoul_test',
	'lakesoul.pg.url'='jdbc:postgresql://lakesoul-meta-pg:5432/lakesoul_test?stringtype=unspecified',
	'minio.endpoint'='http://minio:9000',
	'minio.access_key'='admin',
	'minio.secret_key'='password'
	);