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

suite("map_uniq_with_local_tvf", "p0") {
    def table_name = "map_uniq"
    sql "DROP TABLE IF EXISTS ${table_name};"
    sql """
            CREATE TABLE ${table_name} (
              `id` int(11) NULL,
              `m` MAP<text,text> NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false"
            ); """

    String bucket = getS3BucketName()
    String key = "regression/local_tvf_migration/${UUID.randomUUID()}/mm.orc"
    def s3Client = getS3Client()
    s3Client.putObject(bucket, key, new File(context.config.dataPath, "types/complex_types/mm.orc"))
    try {
        sql """INSERT INTO ${table_name} SELECT id, m FROM S3(
            "uri" = "s3://${bucket}/${key}",
            "format" = "orc",
            "s3.endpoint" = "${getS3Endpoint()}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}"
        )"""
    } finally {
        s3Client.deleteObject(bucket, key)
    }
    sql "sync"
    qt_sql "SELECT COUNT(*) FROM ${table_name}"
    qt_sql  """ select count(m) from ${table_name}; """
    qt_sql  """ select count(m) from ${table_name} where map_size(m) > 0;"""

}
