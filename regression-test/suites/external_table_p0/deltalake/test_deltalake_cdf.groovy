// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


import groovy.json.JsonSlurper
import org.apache.doris.regression.action.ProfileAction
import org.apache.doris.regression.util.JdbcUtils

import java.sql.DriverManager
import java.time.Instant

suite("test_deltalake_cdf", "p0,external,deltalake,external_docker") {
    def cfg = context.config.otherConfigs
    if (!"true".equalsIgnoreCase(cfg.get("enableDeltaLakeTest"))) {
        return
    }
    String host = cfg.get("externalEnvIp")
    String warehouse = cfg.get("deltalakeWarehouse")
    String catalog = "test_deltalake_cdf"
    String db = "delta_cdf_p0"
    String storage = warehouse.startsWith("s3") ? """, 's3.endpoint'='http://${host}:${cfg.get('deltalakeMinioPort')}',
        's3.region'='us-east-1','s3.access_key'='admin','s3.secret_key'='password','use_path_style'='true'""" : ""
    sql "drop catalog if exists ${catalog}"
    sql """create catalog ${catalog} properties ('type'='deltalake','warehouse'='${warehouse}',
            'enable.mapping.timestamp_tz'='true','enable.mapping.varbinary'='true'${storage})"""
    sql "set time_zone='UTC'"
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    DriverManager.getConnection("jdbc:hive2://${host}:${cfg.get('deltalakeSparkPort')}/default",
            "hadoop", "hadoop").withCloseable { spark ->
        def reference = { String q -> JdbcUtils.executeToList(spark, q)[0] }
        def create = { String name, String schema, String partition, Map extra ->
            reference("drop table if exists ${db}.${name}")
            def props = ['delta.enableChangeDataFeed':'true', 'delta.enableInCommitTimestamps':'true',
                         'delta.enableDeletionVectors':'false'] + extra
            String properties = props.collect { k, v -> "'${k}'='${v}'" }.join(",")
            reference("create table ${db}.${name}(${schema}) using delta ${partition} tblproperties(${properties})")
        }
        def refresh = { String name -> sql "refresh table ${catalog}.${db}.${name}" }
        def d = { String name, def start, def end ->
            "table_changes('table'='${catalog}.${db}.${name}','start_version'='${start}'" +
                    (end == null ? ")" : ",'end_version'='${end}')") }
        def s = { String name, def start, def end ->
            "table_changes('${db}.${name}',${start}" + (end == null ? ")" : ",${end})") }
        def verify = { String tag, String dq, String sq ->
            assertSparkDorisResultEquals(reference(sq), sql(dq))
            quickTest(tag, dq)
        }
        def compare = { String tag, String name, def start, def end, String data, String condition ->
            String order = "_commit_version,${data},_change_type"
            String dorisFrom = "from ${d(name,start,end)} ${condition} order by ${order}"
            String sparkFrom = "from ${s(name,start,end)} ${condition} order by ${order}"
            String columns = "${data},_change_type,_commit_version"
            // The clock is writer-generated. Compare its exact milliseconds independently of the golden output.
            assertSparkDorisResultEquals(reference("select ${columns},unix_millis(_commit_timestamp) ${sparkFrom}"),
                    sql("select ${columns},millisecond_timestamp(cast(_commit_timestamp as datetimev2(6))) ${dorisFrom}"))
            quickTest(tag, "select ${columns} ${dorisFrom}")
        }
        reference("set spark.sql.session.timeZone=UTC")
        reference("set spark.sql.shuffle.partitions=2")
        reference("create database if not exists ${db} location '${warehouse}/${db}'")
        for (String mode : ["none","name","id"]) {
            String name = "events_${mode}"
            String t = "${db}.${name}"
            create(name, "id int,p string", "partitioned by(p)", ['delta.columnMapping.mode':mode])
            reference("insert into ${t} values (1,'a b'),(2,'retired'),(3,'a b')")
            reference("update ${t} set id=11 where id=1")
            reference("delete from ${t} where id=2")
            reference("""merge into ${t} t using (select * from values (3,'a b'),(4,'blue') as s(id,p)) s
                      on t.id=s.id when matched then update set id=s.id+10 when not matched then insert *""")
            reference("optimize ${t}")
            sql "refresh catalog ${catalog}"
            compare("actions_${mode}",name,0,null,"id,p","")
            compare("filter_${mode}",name,1,4,"id,p","where p='a b' and _commit_version>=2 and _change_type='update_preimage'")
            verify("physical_filter_${mode}", "select id from ${d(name,1,4)} where _change_type='update_postimage' order by id",
                    "select id from ${s(name,1,4)} where _change_type='update_postimage' order by id")
            compare("retired_partition_${mode}",name,1,4,"id,p","where p='retired'")
            verify("optimize_${mode}", "select count(*) from ${d(name,5,999)}", "select count(*) from ${s(name,5,999)}")
        }

        create("binary_parts","id int,p binary","partitioned by(p)",[:])
        reference("insert into ${db}.binary_parts values(1,cast('A/B' as binary))")
        reference("update ${db}.binary_parts set id=2 where id=1")
        sql "refresh catalog ${catalog}"
        verify("binary_partition", "select id,hex(p),_change_type from ${d('binary_parts',1,2)} order by _commit_version,id",
                "select id,hex(p),_change_type from ${s('binary_parts',1,2)} order by _commit_version,id")

        create("dv","id bigint,p string","",['delta.enableDeletionVectors':'true'])
        reference("insert into ${db}.dv select /*+ coalesce(1) */ id,cast(id as string) from range(8)")
        reference("delete from ${db}.dv where id=1")
        reference("delete from ${db}.dv where id=3")
        reference("restore table ${db}.dv to version as of 2")
        reference("delete from ${db}.dv")
        sql "refresh catalog ${catalog}"
        compare("dv_changes","dv",0,5,"id","")
        for (int version : [1,2,3,4,5]) {
            verify("dv_count_${version}", "select count(*) from ${d('dv',version,version)}",
                    "select count(*) from ${s('dv',version,version)}")
        }
        String nativeFrom = "${catalog}.${db}.`dv\$changes`@incr('start_version'='2','end_version'='4')"
        verify("native", "select id,_change_type,_commit_version from ${nativeFrom} order by _commit_version,id",
                "select id,_change_type,_commit_version from ${s('dv',2,4)} order by _commit_version,id")
        // Delta Spark resolves both named relations to the first range in this self-join. The path form
        // on the identical files preserves both coordinates, matching the two standalone named reads.
        def byPath = { long start, long end -> "table_changes_by_path('${warehouse}/${db}/dv',${start},${end})" }
        verify("join_ranges", "select a.id,a._change_type,b._change_type from ${d('dv',3,3)} a join ${d('dv',4,4)} b on a.id=b.id order by a.id",
                "select a.id,a._change_type,b._change_type from ${byPath(3,3)} a join ${byPath(4,4)} b on a.id=b.id order by a.id")
        verify("shared_end", "select a.c,b.c from (select count(*) c from ${d('dv',3,4)}) a cross join (select count(*) c from ${d('dv',4,4)}) b",
                "select a.c,b.c from (select count(*) c from ${byPath(3,4)}) a cross join (select count(*) c from ${byPath(4,4)}) b")
        quickTest("desc_function","desc function ${d('dv',2,3)}")
        quickTest("desc_changes","desc ${catalog}.${db}.`dv\$changes`")
        def times = reference("select distinct _commit_version,unix_millis(_commit_timestamp) from ${s('dv',1,5)} order by _commit_version")
                .collectEntries { [(it[0] as long): it[1] as long] }
        String begin = Instant.ofEpochMilli(times[2L]).toString()
        String finish = Instant.ofEpochMilli(times[4L]).toString()
        for (def bounds : [timestamps:"'start_timestamp'='${begin}','end_timestamp'='${finish}'",
                           version_time:"'start_version'='2','end_timestamp'='${finish}'",
                           time_version:"'start_timestamp'='${begin}','end_version'='4'"]) {
            verify(bounds.key, "select id,_change_type,_commit_version from table_changes('table'='${catalog}.${db}.dv',${bounds.value}) order by _commit_version,id",
                    "select id,_change_type,_commit_version from ${s('dv',2,4)} order by _commit_version,id")
        }
        sql "set time_zone='America/Los_Angeles'"
        reference("set spark.sql.session.timeZone=America/Los_Angeles")
        assertSparkDorisResultEquals(reference("select id,hour(_commit_timestamp) from ${s('dv',2,4)} order by _commit_version,id"),
                sql("select id,hour(cast(_commit_timestamp as datetimev2(6))) from ${d('dv',2,4)} order by _commit_version,id"))
        reference("set spark.sql.session.timeZone=UTC")
        qt_la "select id,_commit_version from table_changes('table'='${catalog}.${db}.dv','start_timestamp'='${begin}','end_timestamp'='${finish}') order by _commit_version,id"
        sql "set time_zone='UTC'"
        def mid = Instant.ofEpochMilli((times[2L]+times[3L]).intdiv(2) as long).toString()
        qt_gap "select count(*) from table_changes('table'='${catalog}.${db}.dv','start_timestamp'='${mid}','end_timestamp'='${mid}')"
        compare("filtered_timestamp","dv",1,5,"id","where _commit_timestamp >= timestamp '${begin.replace('T',' ').replace('Z','')}'")

        for (String mode : ["none","name","id"]) {
            String name = "evolve_${mode}"
            String t = "${db}.${name}"
            create(name,"id int","",['delta.columnMapping.mode':mode,'delta.enableTypeWidening':'true'])
            reference("insert into ${t} values (1)")
            reference("alter table ${t} add columns(label string)")
            reference("insert into ${t} values (2,'two')")
            sql "refresh catalog ${catalog}"
            compare("add_${mode}",name,1,3,"id,label","")
            // All Doris mapping modes use the end-version schema, including an unmapped table after ADD.
            quickTest("historical_schema_${mode}", "desc function ${d(name,1,1)}")
            reference("alter table ${t} alter column id type bigint")
            reference("insert into ${t} values (6000000000,'wide')")
            refresh(name)
            compare("wide_${mode}",name,4,5,"id,label","")
            test { sql "select * from ${d(name,3,5)}"; exception "Incompatible change data feed schema" }
            if (mode != "none") {
                reference("alter table ${t} rename column label to note")
                reference("alter table ${t} drop column note")
                refresh(name)
                test { sql "select * from ${d(name,5,6)}"; exception "Incompatible change data feed schema" }
                test { sql "select * from ${d(name,6,7)}"; exception "Incompatible change data feed schema" }
            }
            // Remove a whole original INT file after widening. CDF must read its id as BIGINT.
            reference("delete from ${t} where id=1")
            refresh(name)
            long removed = reference("describe history ${t} limit 1")[0][0] as long
            compare("remove_old_int_${mode}",name,removed,removed,"id","")
        }
        create("switches","id int","",[:])
        reference("insert into ${db}.switches values (1)")
        reference("alter table ${db}.switches set tblproperties('delta.enableChangeDataFeed'='false')")
        reference("insert into ${db}.switches values (2)")
        reference("alter table ${db}.switches set tblproperties('delta.enableChangeDataFeed'='true')")
        reference("insert into ${db}.switches values (3)")
        sql "refresh catalog ${catalog}"
        test { sql "select * from ${d('switches',1,5)}"; exception "was not enabled" }
        compare("reenabled","switches",4,null,"id","")
        for (def bad : [
            ["select * from ${d('dv',4,2)}","end cannot be before start"],
            ["select * from ${d('dv',99,100)}","outside retained log"],
            ["select * from table_changes('table'='${catalog}.${db}.dv','end_version'='1')","one start"],
            ["select * from table_changes('table'='${catalog}.${db}.dv','db'='${db}','start_version'='1')","Conflicting"],
            ["select * from table_changes('table'='${catalog}.${db}.dv','start_version'='1','typo'='1')","Unknown"],
            ["select * from ${catalog}.${db}.`dv\$changes`","requires @incr"],
            ["select * from ${catalog}.${db}.dv@incr('start_version'='1')","do not support @incr"],
            ["select * from ${catalog}.${db}.`dv\$changes` for version as of 2","does not support"],
            ["select * from ${catalog}.${db}.`dv\$changes` for time as of '${begin}'","does not support"]]) {
            test { sql bad[0]; exception bad[1] }
        }

        sql "drop user if exists 'delta_cdf_reader'@'%'"
        sql "create user 'delta_cdf_reader'@'%' identified by 'CdfFixture_0123'"
        if (isCloudMode()) {
            def cluster = sql("show clusters")[0][0]
            sql "grant usage_priv on cluster `${cluster}` to 'delta_cdf_reader'@'%'"
        }
        sql "create database if not exists internal.regression_test"
        sql "grant select_priv on internal.regression_test.* to 'delta_cdf_reader'@'%'"
        connect("delta_cdf_reader", "CdfFixture_0123", context.config.jdbcUrl) {
            test { sql "select * from ${d('dv',2,3)}"; exception "denied" }
            test { sql "desc function ${d('dv',2,3)}"; exception "denied" }
        }
        sql "grant select_priv on ${catalog}.${db}.dv to 'delta_cdf_reader'@'%'"
        connect("delta_cdf_reader", "CdfFixture_0123", context.config.jdbcUrl) {
            quickTest("authorized","select id,_commit_version from ${d('dv',2,3)} order by _commit_version,id")
        }

        sql "set enable_sql_cache=true"
        String openFeed = "select id,_change_type from ${d('switches',4,null)} order by id"
        quickTest("open_before_append",openFeed)
        reference("insert into ${db}.switches values (4)")
        quickTest("open_after_append",openFeed)

        reference("set spark.sql.variant.allowReadingShredded=true")
        reference("set spark.sql.variant.writeShredding.enabled=true")
        reference("set spark.sql.variant.forceShreddingSchemaForTest=a int, b string")
        create("variants","id int,v variant","",['delta.enableVariantShredding':'true'])
        reference("""insert into ${db}.variants select /*+ coalesce(1) */ id,parse_json(j) from values
                     (1,'{"a":10,"b":"first"}'),(2,'{"a":20}') as t(id,j)""")
        sql "refresh catalog ${catalog}"
        String token = UUID.randomUUID().toString()
        String variantSql = "select /* ${token} */ id,cast(v['a'] as int),_change_type from ${d('variants',1,1)} order by id"
        sql "set enable_profile=true"
        explain { sql(variantSql); contains("all access paths:"); contains("v.a") }
        verify("variant_leaf",variantSql,"select id,variant_get(v,'\$.a','int'),_change_type from ${s('variants',1,1)} order by id")
        def keys = ["VariantLeafProjectionRowGroupColumns","VariantResidualProjectionRowGroupColumns",
                    "VariantFullProjectionRowGroupColumns"]
        String profile = new ProfileAction(context).getProfileBySql(token,keys)
        def counters = keys.collect { key ->
            (profile =~ /- ${key}:\s+([0-9,]+)/).collect { it[1].replace(",","").toLong() }.sum(0L)
        }
        logger.info("CDF Variant leaf/residual/full row-group columns ${counters}")
        assertTrue(counters[0] > 0 && counters[1] == 0 && counters[2] == 0, counters.toString())
        sql "set enable_profile=false"
    }

    String uri = cfg.get("deltalakeUcUri")
    def json = new JsonSlurper()
    def staged = json.parse(new URL("${uri}/api/2.1/unity-catalog/delta/v1/catalogs/doris_delta/schemas/m1/tables/uc_managed_cdf"))
    long version = staged["latest-table-version"]
    def last = staged.commits.find { it.version == version }
    def location = new File(new URI(staged.metadata.location))
    assertTrue(new File(location,"_delta_log/_staged_commits/${last['file-name']}").isFile())
    assertFalse(new File(location, String.format("_delta_log/%020d.json",version)).exists())
    sql "drop catalog if exists test_deltalake_cdf_uc"
    sql """create catalog test_deltalake_cdf_uc properties('type'='deltalake','deltalake.catalog.type'='uc',
            'deltalake.uc.uri'='${uri}','deltalake.uc.catalog'='doris_delta')"""
    String feed = "table_changes('table'='test_deltalake_cdf_uc.m1.uc_managed_cdf','start_version'='0','end_version'='${version}')"
    String fields = "id,p,_change_type,_commit_version"
    assertSparkDorisResultEquals(json.parse(new File(location,"_doris_cdf_reference.json")),
            sql("select ${fields},millisecond_timestamp(cast(_commit_timestamp as datetimev2(6))) from ${feed} order by _commit_version,id,_change_type"))
    quickTest("uc_staged", "select ${fields} from ${feed} order by _commit_version,id,_change_type")
}
