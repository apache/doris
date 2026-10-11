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

suite("test_jdbc_native_uuid", "p0,external") {
    if (!context.config.otherConfigs.get("enableJdbcTest")?.toString()?.equalsIgnoreCase("true")) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("pg_14_port")
    String bucket = getS3BucketName()
    String s3_endpoint = getS3Endpoint()
    String driver = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"
    sql "DROP CATALOG IF EXISTS jdbc_native_uuid"
    sql """CREATE CATALOG jdbc_native_uuid PROPERTIES (
        "type"="jdbc", "user"="postgres", "password"="123456",
        "jdbc_url"="jdbc:postgresql://${host}:${port}/postgres?currentSchema=public&useSSL=false",
        "driver_url"="${driver}", "driver_class"="org.postgresql.Driver")"""
    def remoteExecute = { String query ->
        sql("CALL EXECUTE_STMT('jdbc_native_uuid', '" + query.replace("'", "''") + "')")
    }
    remoteExecute("DROP TABLE IF EXISTS public.native_uuid_roundtrip")
    remoteExecute("CREATE TABLE public.native_uuid_roundtrip (id INT, u UUID, a UUID[])")
    remoteExecute("INSERT INTO public.native_uuid_roundtrip VALUES " +
            "(1, '00112233-4455-6677-8899-aabbccddeeff', " +
            "ARRAY['80000000-0000-0000-0000-000000000000'::uuid, NULL]), (2, NULL, NULL)")
    qt_schema "DESC jdbc_native_uuid.public.native_uuid_roundtrip"
    qt_values "SELECT * FROM jdbc_native_uuid.public.native_uuid_roundtrip ORDER BY id"
    sql "DROP TABLE IF EXISTS jdbc_native_uuid_source"
    sql """CREATE TABLE jdbc_native_uuid_source (id INT, u UUID) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")"""
    sql "INSERT INTO jdbc_native_uuid_source VALUES (3, 'ffffffff-ffff-ffff-ffff-ffffffffffff'), (4, NULL)"
    sql "INSERT INTO jdbc_native_uuid.public.native_uuid_roundtrip(id, u) SELECT id, u FROM jdbc_native_uuid_source"
    qt_roundtrip "SELECT id, u FROM jdbc_native_uuid.public.native_uuid_roundtrip ORDER BY id"
}
