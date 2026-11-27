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


import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import groovy.json.JsonSlurper
import org.apache.doris.regression.action.ProfileAction
import org.apache.doris.regression.util.JdbcUtils
import org.apache.parquet.format.FileMetaData
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.transport.TIOStreamTransport

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.sql.DriverManager

suite("test_deltalake_variant", "p0,external,deltalake,external_docker") {
    def cfg = context.config.otherConfigs
    if (!"true".equalsIgnoreCase(cfg.get("enableDeltaLakeTest"))) {
        return
    }
    String host = cfg.get("externalEnvIp")
    String warehouse = cfg.get("deltalakeWarehouse")
    String endpoint = "http://${host}:${cfg.get('deltalakeMinioPort')}"
    String catalog = "test_deltalake_variant"
    String db = "delta_variant_p0"
    String storage = warehouse.startsWith("s3") ? """, 's3.endpoint'='${endpoint}', 's3.region'='us-east-1',
        's3.access_key'='admin','s3.secret_key'='password','use_path_style'='true'""" : ""
    sql "drop catalog if exists ${catalog}"
    sql """create catalog ${catalog} properties ('type'='deltalake', 'warehouse'='${warehouse}',
        'enable.mapping.timestamp_tz'='true', 'enable.mapping.varbinary'='true'${storage})"""
    sql "set time_zone='UTC'"
    sql "set enable_sql_cache=false"

    Class.forName("org.apache.hive.jdbc.HiveDriver")
    DriverManager.getConnection("jdbc:hive2://${host}:${cfg.get('deltalakeSparkPort')}/default",
            "hadoop", "hadoop").withCloseable { spark ->
        def sparkSql = { String statement -> JdbcUtils.executeToList(spark, statement)[0] }
        def compare = { String tag, String dorisSql, String reference ->
            assertSparkDorisResultEquals(sparkSql(reference), sql(dorisSql))
            quickTest(tag, dorisSql)
        }
        def refresh = { String name -> sql "refresh table ${catalog}.${db}.${name}" }
        def create = { String name, String schema, String options ->
            sparkSql("drop table if exists ${db}.${name}")
            sparkSql("create table ${db}.${name} (${schema}) using delta ${options}")
        }
        def version = { String name -> sparkSql("describe history ${db}.${name} limit 1")[0][0] as long }
        sparkSql("set spark.sql.session.timeZone=UTC")
        sparkSql("set spark.databricks.delta.variant.forceUsePreviewTableFeature=false")
        sparkSql("set spark.databricks.delta.variantShredding.forceUsePreviewTableFeature=true")
        sparkSql("set spark.sql.variant.allowReadingShredded=true")
        sparkSql("set spark.sql.variant.writeShredding.enabled=false")
        sparkSql("create database if not exists ${db} location '${warehouse}/${db}'")

        // Compare every row of all six logical shapes from the v2 checkpoint, including null container elements.
        sparkSql("""create table if not exists ${db}.variant_golden using delta
                    location '${warehouse}/delta_golden/spark_variant_checkpoint'""")
        def shapes = ["v", "array_of_variants", "struct_of_variants", "map_of_variants",
                      "array_of_struct_of_variants", "struct_of_array_of_variants"]
        String dorisGolden = "${catalog}.delta_golden.spark_variant_checkpoint"
        def parser = new JsonSlurper()
        def jsonRows = { rows -> rows.collect { row -> [row[0] as long] + row.drop(1).collect {
            it == null ? null : parser.parseText(it.toString())
        } } }
        def sparkShapes = shapes.collect { it == "v" ? "to_json(v)" :
                "to_json(${it}, map('ignoreNullFields','false'))" }.join(",")
        String dorisShapes = shapes.collect { "to_json(${it})" }.join(",")
        def goldenRows = jsonRows(sparkSql("select id,${sparkShapes} from ${db}.variant_golden order by id"))
        assertEquals(102, goldenRows.size())
        assertEquals(goldenRows, jsonRows(sql("select id,${dorisShapes} from ${dorisGolden} order by id")))
        qt_golden_shapes "select id,${dorisShapes} from ${dorisGolden} where id=99 order by id"

        create("plain", "id int,v variant", "")
        sparkSql("""insert into ${db}.plain select id,parse_json(j) from values (0,cast(null as string)),(1,'null'),
            (2,'{}'),(3,'[10,20]'),(4,'{"deep":{"items":[{"x":9}]},"8":88}') as t(id,j)""")
        sparkSql("""insert into ${db}.plain select 5,to_variant_object(named_struct(
            'bool',true,'tiny',cast(-128 as tinyint),'small',cast(-32768 as smallint),'i',2147483647,
            'long',cast('-9223372036854775808' as bigint),'f',cast('NaN' as float),'d',cast('Infinity' as double),
            'dec',cast('-1234567890.1234' as decimal(20,4)),'day',date'1970-01-02',
            'ts',timestamp'2024-01-02 03:04:05.123456','ntz',cast('2024-01-02 03:04:05.654321' as timestamp_ntz),
            'bytes',cast('binary' as binary),'text','hello','nil',cast(null as int)))""")
        sql "refresh catalog ${catalog}"
        String plain = "${catalog}.${db}.plain"
        for (def entry : [is_null:"v is null", not_null:"v is not null"]) {
            compare(entry.key, "select id from ${plain} where ${entry.value} order by id",
                    "select id from ${db}.plain where ${entry.value} order by id")
        }
        // Spark 4.0 rejects equality on VARIANT. IS NULL gives the row set for null-safe equality to NULL.
        compare("null_safe", "select id from ${plain} where v <=> null order by id",
                "select id from ${db}.plain where v is null order by id")
        compare("root_nulls", "select id, v is null, to_json(v) from ${plain} where id<=3 order by id",
                "select id,v is null,to_json(v) from ${db}.plain where id<=3 order by id")
        compare("array_and_keys", """select id,cast(v[1] as int),cast(v['8'] as int),cast(v['deep']['items'][1]['x'] as int),
                    v['missing'] is null from ${plain} where id in (3,4) order by id""",
                """select id,try_variant_get(v,'\$[0]','int'),try_variant_get(v,'\$["8"]','int'),
                    try_variant_get(v,'\$.deep.items[0].x','int'),try_variant_get(v,'\$.missing','variant') is null
                    from ${db}.plain where id in (3,4) order by id""")
        def dFields = []
        def sFields = []
        [bool:"boolean",tiny:"tinyint",small:"smallint",i:"int",long:"bigint",dec:"decimal(20,4)",text:"string"].each { k, type ->
            dFields << "cast(v['${k}'] as ${type})"
            sFields << "variant_get(v,'\$.${k}','${type}')"
        }
        dFields += ["isnan(cast(v['f'] as float))","isinf(cast(v['d'] as double))","cast(v['day'] as date)",
                    "cast(v['ts'] as datetimev2(6))","cast(v['ntz'] as datetimev2(6))","hex(from_base64(json_extract_string(cast(v['bytes'] as string),'\$')))","v['nil'] is null"]
        sFields += ["isnan(variant_get(v,'\$.f','float'))","variant_get(v,'\$.d','double')=cast('Infinity' as double)",
                    "variant_get(v,'\$.day','date')","cast(variant_get(v,'\$.ts','timestamp') as string)",
                    "cast(variant_get(v,'\$.ntz','timestamp_ntz') as string)","hex(variant_get(v,'\$.bytes','binary'))",
                    "variant_get(v,'\$.nil','variant') is null"]
        compare("types", "select ${dFields.join(',')} from ${plain} where id=5",
                "select ${sFields.join(',')} from ${db}.plain where id=5")

        String mixed = "${catalog}.${db}.mixed"
        create("mixed","id int,g int,v variant","partitioned by(g) tblproperties('delta.enableDeletionVectors'='true')")
        sparkSql("""insert into ${db}.mixed select /*+ COALESCE(1) */ 10,0,parse_json('{"a":10,"b":"plain"}')""")
        long ordinaryVersion = version("mixed")
        sparkSql("alter table ${db}.mixed set tblproperties('delta.enableVariantShredding'='true')")
        sparkSql("set spark.sql.variant.writeShredding.enabled=true")
        sparkSql("set spark.sql.variant.forceShreddingSchemaForTest=a int, b string")
        sparkSql("""insert into ${db}.mixed select /*+ COALESCE(1) */ id,1,parse_json(j) from values
                    (20,'{"a":20,"b":"keep"}'),(21,'{"a":21}'),(22,'{"a":22}') as t(id,j)""")
        sparkSql("""insert into ${db}.mixed select /*+ COALESCE(1) */ id,2,parse_json(j) from values
                    (30,'{"a":"text","b":"mixed"}'),(31,'{"a":31,"other":true}') as t(id,j)""")
        refresh("mixed")
        def mixedRows = { String tag, String dorisSuffix, String sparkSuffix ->
            compare(tag, "select id,g,to_json(v) from ${mixed} ${dorisSuffix} order by id",
                    "select id,g,to_json(v) from ${db}.mixed ${sparkSuffix} order by id")
        }
        mixedRows("mixed_files","","")
        mixedRows("before_shredding","for version as of ${ordinaryVersion}","version as of ${ordinaryVersion}")
        def verifyFeatures = { String name, List<String> expected ->
            String detail = "${catalog}.${db}.`" + name + '$detail`'
            def features = parser.parseText(sql("select to_json(tableFeatures) from ${detail}")[0][0] as String)
            assertEquals(expected.sort(), features.findAll { it.startsWith("variant") }.sort())
        }
        verifyFeatures("plain",["variantType"])
        verifyFeatures("mixed",["variantType","variantShredding-preview"])

        // Verify physical leaves independently of Delta's feature declarations and the Doris reader.
        def s3 = warehouse.startsWith("s3") ? AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
                .withPathStyleAccessEnabled(true)
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("admin", "password"))).build() : null
        try {
            def files = sql('select partition_values[\'g\'],path from ' + catalog + '.' + db + '.`mixed$files` order by 1')
            assertEquals(["0", "1", "2"], files.collect { it[0].toString() })
            files.each { row ->
                URI uri = new URI(row[1].toString())
                byte[] bytes = s3 == null ? new File(uri.path).bytes :
                        s3.getObject(uri.host, uri.path.substring(1)).withCloseable { it.objectContent.bytes }
                int length = ByteBuffer.wrap(bytes, bytes.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt()
                def footer = new FileMetaData()
                def input = new ByteArrayInputStream(bytes, bytes.length - 8 - length, length)
                footer.read(new TCompactProtocol(new TIOStreamTransport(input)))
                def paths = footer.row_groups.collectMany { it.columns.collect { c -> c.meta_data.path_in_schema.join('.') } }
                assertTrue(paths.containsAll(["v.metadata", "v.value"]), paths.toString())
                assertEquals(row[0].toString() != "0", paths.contains("v.typed_value.a.typed_value"))
            }
        } finally {
            s3?.shutdown()
        }

        def profiles = new ProfileAction(context)
        def counter = { String profile, String key ->
            def values = profile =~ /- ${key}:\s+([^\n]+)/
            long total = 0
            while (values.find()) {
                def v = values.group(1)
                def exact = v =~ /\(([0-9,]+)\)/
                total += (exact.find() ? exact.group(1) : (v =~ /([0-9,]+)/)[0][1]).replace(",","").toLong()
            }
            total
        }
        sql "set enable_profile=true"
        for (int group : [1,2]) {
            String token = UUID.randomUUID().toString()
            String q = "select /* ${token} */ id,cast(v['a'] as string) from ${mixed} where g=${group} order by id"
            explain {
                sql(q)
                contains("all access paths: [v.a]")
            }
            compare("leaf_${group}",q,"select id,variant_get(v,'\$.a','string') from ${db}.mixed where g=${group} order by id")
            def keys = ["VariantLeafProjectionRowGroupColumns","VariantResidualProjectionRowGroupColumns",
                        "VariantFullProjectionRowGroupColumns"]
            String profile = profiles.getProfileBySql(token, keys)
            def counts = keys.collect { counter(profile, it) }
            logger.info("Delta Variant partition ${group}, leaf/residual/full row-group columns ${counts}")
            assertTrue(counts[0] > 0 && counts[2] == 0, counts.toString())
            assertEquals(group == 2, counts[1] > 0)
        }
        sql "set enable_profile=false"
        compare("residual_filter", "select id from ${mixed} where g=2 and cast(v['a'] as string)='text' order by id",
                "select id from ${db}.mixed where g=2 and variant_get(v,'\$.a','string')='text' order by id")
        long beforeDelete = version("mixed")
        sparkSql("delete from ${db}.mixed where g=1 and id=20")
        refresh("mixed")
        qt_dv '''select cast(operationMetrics['numDeletionVectorsAdded'] as bigint)>0
                from test_deltalake_variant.delta_variant_p0.`mixed$history` where operation='DELETE' order by version'''
        mixedRows("after_delete","","")
        mixedRows("before_delete","for version as of ${beforeDelete}","version as of ${beforeDelete}")
        // Synchronous planning exposes the exact log-derived count after subtracting the DV.
        String countQuery = "select /*+ SET_VAR(enable_external_table_batch_mode=false) */ count(*) from ${mixed}"
        explain {
            sql(countQuery)
            contains("pushdown agg=COUNT (5)")
        }
        compare("count", countQuery, "select count(*) from ${db}.mixed")

        for (boolean preview : [true,false]) {
            String name = preview ? "preview" : "stable"
            sparkSql("set spark.databricks.delta.variant.forceUsePreviewTableFeature=${preview}")
            sparkSql("set spark.databricks.delta.variantShredding.forceUsePreviewTableFeature=${preview}")
            create(name,"id int,v variant","tblproperties('delta.enableVariantShredding'='true')")
            sparkSql("""insert into ${db}.${name} values (1,parse_json('{"a":7}'))""")
            sql "refresh catalog ${catalog}"
            verifyFeatures(name,preview ? ["variantType-preview","variantShredding-preview"] : ["variantType","variantShredding"])
            compare(name,"select id,cast(v['a'] as int) from ${catalog}.${db}.${name} order by id",
                    "select id,variant_get(v,'\$.a','int') from ${db}.${name} order by id")
        }
        sparkSql("set spark.sql.variant.writeShredding.enabled=false")
        sparkSql("reset spark.sql.variant.forceShreddingSchemaForTest")

        for (String mode : ["none","name","id"]) {
            String name = "mapped_${mode}"
            String source = "${db}.${name}"
            String table = "${catalog}.${source}"
            String shape = """id int,v variant,s struct<price:decimal(15,4),instant:timestamp,wall:timestamp_ntz,
                `8`:int,other:int,bytes:binary,txt:string,payload:variant>,arr array<variant>,m map<string,variant>,
                ordinary struct<value:binary,metadata:binary>"""
            create(name,shape,"tblproperties('delta.columnMapping.mode'='${mode}')")
            def insert = { int id, String member ->
                sparkSql("""insert into ${source} select ${id},parse_json('{"8":88,"items":[11,12]}'),
                    named_struct('price',cast(12345.0001 as decimal(15,4)),'instant',timestamp'2024-03-04 05:06:07.123456',
                    'wall',cast('2024-03-04 05:06:07.654321' as timestamp_ntz),'8',${id}+70,'other',${id}+800,
                    'bytes',unhex('00FF80'),'txt','hello','${member}',parse_json('{"x":7}')),
                    array(parse_json('{"x":12}')),map('k',parse_json('{"x":17}')),
                    named_struct('value',unhex('FE01'),'metadata',unhex('0002'))""")
            }
            insert(1, "payload")
            sql "refresh catalog ${catalog}"
            long original = version(name)
            def verifyMapped = { String tag, String v, String member, String ds, String ss ->
                compare(tag, """select id,struct_element(s,'8'),struct_element(s,'other'),struct_element(s,'price'),
                    cast(struct_element(s,'instant') as datetimev2(6)),struct_element(s,'wall'),
                    hex(struct_element(s,'bytes')),struct_element(s,'txt'),cast(${v}['8'] as int),
                    cast(${v}['items'][1] as int),cast(struct_element(s,'${member}')['x'] as int),
                    cast(arr[1]['x'] as int),cast(m['k']['x'] as int),hex(struct_element(ordinary,'value')),
                    hex(struct_element(ordinary,'metadata')) from ${table} ${ds} order by id""",
                    """select id,s.`8`,s.other,s.price,cast(s.instant as string),cast(s.wall as string),hex(s.bytes),s.txt,
                    variant_get(${v},'\$["8"]','int'),variant_get(${v},'\$.items[0]','int'),variant_get(s.${member},'\$.x','int'),
                    variant_get(arr[0],'\$.x','int'),variant_get(m['k'],'\$.x','int'),hex(ordinary.value),hex(ordinary.metadata)
                    from ${source} ${ss} order by id""")
            }
            explain {
                sql("select struct_element(s,'8') from ${table}")
                contains("all access paths: [s.8]")
            }
            compare("${mode}_numeric","select struct_element(s,'8') from ${table} order by id",
                    "select s.`8` from ${source} order by id")
            verifyMapped(mode, "v", "payload", "", "")
            // The instant follows the session zone, while timestamp_ntz keeps its wall clock.
            sql "set time_zone='America/Los_Angeles'"
            sparkSql("set spark.sql.session.timeZone=America/Los_Angeles")
            compare("${mode}_time_zone", """select cast(struct_element(s,'instant') as datetimev2(6)),
                    struct_element(s,'wall') from ${table} order by id""",
                    "select cast(s.instant as string),cast(s.wall as string) from ${source} order by id")
            sql "set time_zone='UTC'"
            sparkSql("set spark.sql.session.timeZone=UTC")
            if (mode != "none") {
                sparkSql("alter table ${source} rename column v to renamed")
                sparkSql("alter table ${source} rename column s.payload to renamed_inner")
                insert(2, "renamed_inner")
                refresh(name)
                verifyMapped("${mode}_renamed", "renamed", "renamed_inner", "", "")
                verifyMapped("${mode}_history", "v", "payload", "for version as of ${original}", "version as of ${original}")
            }
        }
        create("evolved","id int","")
        sparkSql("insert into ${db}.evolved values (1)")
        sparkSql("alter table ${db}.evolved add columns(v variant)")
        sparkSql("""insert into ${db}.evolved values (2,parse_json('{"a":2}'))""")
        sql "refresh catalog ${catalog}"
        compare("added_column","select id,v is null,cast(v['a'] as int) from ${catalog}.${db}.evolved order by id",
                "select id,v is null,variant_get(v,'\$.a','int') from ${db}.evolved order by id")
    }
}
