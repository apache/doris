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

suite("test_gen_iceberg_by_api", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_gen_iceberg_by_api"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """switch ${catalog_name};"""
    sql """ use `test_db`; """

    def q01 = {
        qt_q01 """ select * from multi_partition2 order by val """
        qt_q02 """ select count(*) from table_with_append_file where MAN_ID is not null """
    }

    q01()
}

/*

// 打包后，在iceberg-spark的docker中运行

package org.example;


import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CreateTable {
    public static void main(String[] args) throws Exception {
        CreateTable createTable = new CreateTable();
        createTable.create();
    }

    public void create() {
        HashMap<String, String> prop = new HashMap<>();
        prop.put("uri", "http://172.21.0.101:18181");
        prop.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        prop.put("type", "rest");
        prop.put("AWS_ACCESS_KEY_ID", "admin");
        prop.put("AWS_SECRET_ACCESS_KEY", "password");
        prop.put("AWS_REGION", "us-east-1");
        prop.put("s3.endpoint", "http://172.21.0.101:19001");
        // RESTCatalog catalog = new RESTCatalog();
        Catalog catalog = CatalogUtil.buildIcebergCatalog("df", prop, null);
        catalog.initialize("ddf", prop);
        List<TableIdentifier> test_db = catalog.listTables(Namespace.of("test_db"));
        System.out.println(test_db);

        table1(catalog);
        table2(catalog);

    }
    
    public void table1(Catalog catalog) {

        TableIdentifier of = TableIdentifier.of("test_db", "multi_partition2");
        boolean exists = catalog.tableExists(of);
        Table table;
        if (exists) {
            catalog.dropTable(of);
        }
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "ts", Types.TimestampType.withZone()),
                    Types.NestedField.required(3, "val", Types.StringType.get()));
            PartitionSpec spec = PartitionSpec.builderFor(schema)
                    .identity("id")
                    .hour("ts")
                    .build();
            table = catalog.createTable(of, schema, spec);

        DataFile build = DataFiles.builder(table.spec())
                .withPartitionValues(new ArrayList<String>(){{add("1");add("1000");}})
                .withInputFile(table.io().newInputFile("s3://warehouse/data/multi_partition2/00000-0-f309508c-953a-468f-8bcf-d910b2c7a1e5-00001.parquet"))
                .withFileSizeInBytes(884)
                .withRecordCount(1)//read the record count
                .withFormat(FileFormat.PARQUET)
                .build();
        DataFile build2 = DataFiles.builder(table.spec())
                .withPartitionValues(new ArrayList<String>(){{add("1");add("1000");}})
                .withInputFile(table.io().newInputFile("s3://warehouse/data/multi_partition2/00000-1-4acbae74-1265-4c3b-a8d1-d773037e8b42-00001.parquet"))
                .withFileSizeInBytes(884)
                .withRecordCount(1)//read the record count
                .withFormat(FileFormat.PARQUET)
                .build();
        DataFile build3 = DataFiles.builder(table.spec())
                .withPartitionValues(new ArrayList<String>(){{add("2");add("2000");}})
                .withInputFile(table.io().newInputFile("s3://warehouse/data/multi_partition2/00000-2-9c8199d3-9bc5-4e57-84d7-ad1cedcfbe94-00001.parquet"))
                .withFileSizeInBytes(884)
                .withRecordCount(1)//read the record count
                .withFormat(FileFormat.PARQUET)
                .build();
        DataFile build4 = DataFiles.builder(table.spec())
                .withPartitionValues(new ArrayList<String>(){{add("2");add("2000");}})
                .withInputFile(table.io().newInputFile("s3://warehouse/data/multi_partition2/00000-3-c042039c-f716-41a1-a857-eedf23f7be92-00001.parquet"))
                .withFileSizeInBytes(884)
                .withRecordCount(1)//read the record count
                .withFormat(FileFormat.PARQUET)
                .build();

        table.newAppend()
                .appendFile(build)
                .appendFile(build2)
                .appendFile(build3)
                .appendFile(build4)
                .commit();
    }

    public void table2(Catalog catalog) {
        TableIdentifier of = TableIdentifier.of("test_db", "table_with_append_file");
        boolean exists = catalog.tableExists(of);
        if (exists) {
            catalog.dropTable(of);
        }
            Schema schema = new Schema(
                    Types.NestedField.required(1, "START_TIME", Types.LongType.get()),
                    Types.NestedField.required(2, "ENT_TIME", Types.LongType.get()),
                    Types.NestedField.required(3, "MAN_ID", Types.StringType.get()),
                    Types.NestedField.required(4, "END_TIME_MICROS", Types.TimestampType.withZone()));
            PartitionSpec spec = PartitionSpec.builderFor(schema)
                    .hour("END_TIME_MICROS")
                    .build();
        Table table = catalog.createTable(of, schema, spec);

        InputFile inputFile = table.io()
                .newInputFile("s3://warehouse/data/table_with_append_file/sample-data.snappy.parquet");

        DataFile build = DataFiles.builder(table.spec())
                .withPartitionValues(new ArrayList<String>(){{add("3");}})
                .withInputFile(inputFile)
                .withFileSizeInBytes(inputFile.getLength())
                .withRecordCount(1)//read the record count
                .withFormat(FileFormat.PARQUET)
                .build();

        table.newAppend()
                .appendFile(build)
                .commit();
    }
}


*/