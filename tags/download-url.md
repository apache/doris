# tag 0.8.2.1 (20181027)

download:

http://palo-opensource-bj.bj.bcebos.com/palo-0.8.2.1-release-20181027.tar.gz?authorization=bce-auth-v1/069fc2786e464e63a5f1183824ddb522/2018-10-27T09:06:12Z/-1/host/d9ebd01de7e266946e6dd1f2fb6951e92d4a8cd0a7b437253f9e0dcc3efe7039

1. Added:

   * Using MAVEN instead of ANT to build Frontend and Apache HDFS broker project.
   * Add 2 new proc '/current\_queries' and '/current\_backend\_instances' to monitor the current running queries.
   * Add a manual compaction api on Backend to trigger cumulative and base compaction manually.
   * Add Frontend config 'max\_bytes\_per\_broker\_scanner' to limit the loading bytes per one broker scanner. This is to limit the memory cost of a single broker load job.
   * Add Frontend config 'max\_unfinished\_load\_job' to limit load job number. If number of running load jobs exceed the limit, no more load job is allowed to be submmitted.
   * Exposure backend info to user when encounter errors on Backend, for debugging it more convenient.
   * Add 3 new metrics of Backends: host\_fd\_metrics, process\_fd\_metrics and process\_thread\_metrics, to monitor open files number and threads number.
   * Support getting column size and precision info of table or view using JDBC.

2. Updated:

   * Hide password and other sensitive information in fe.log and fe.audit.log.
   * Change the promethues type name 'GAUGE' to lowercase, to fit the latest promethues version.
   * Backend ip saved in FE will be compared with BE's local ip when heartbeating, to avoid false positive heartbeat response.
   * Using version\_num of tablet instead of calculating nice value to select cumulative compaction candicates.

3. Fixed

   * Fix privilege logic error:
      1. No one can set root password expect for root user itself.
      2. NODE\_PRIV cannot be granted.
      3. ADMIN\_PRIV and GRANT\_PRIV can only be granted or revoked on \*.\*.
      4. No one can modifly privs of default role 'operator' and 'admin'.
      5. No user can be granted to role 'operator'.
   * Missing password and auth check when handling mini load request in Frontend.
   * DomainResolver should start after Frontend transfering to a certain ROLE, not in Catalog construction methods.
   * Fix a bug that read null data twice:
        When reading data with a null value, in some cases, the same data will be read twice by the storage engine,
        resulting in a wrong result. The reason for this problem is that when splitting,
        and the start key is the minimum value, the data with null is read.
   * Fixed a mem leak of using ByteBuf when parsing auth info of http request.
   * Backend process should crash if failed to saving tablet's header.
   * Should remove fd from map when input stream or output stream is closed in Broker process.
   * Predicates should not be pushed down to subquery which contains limit clause.
   * Fix the formula of calculating BE load score.
   * Fix a bug that in some edge cases, non-master Fontend may wait for a unnecessary long timeout after forwarding cmd to Master FE.
   * Fix a bug that granting privs on more than one table does not work.
   * Support 'Insert into' table which contains HLL columns.
   * ExportStmt's toSql() method may throw NullPointer Exception if table does not exist.
   * Remove unnecessary 'get capacity' operation to avoid IO impact.
