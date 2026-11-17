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

suite("test_deltalake_errors", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    sql "drop catalog if exists test_deltalake_errors"
    sql """create catalog test_deltalake_errors properties (
        'type'='deltalake', 'warehouse'='${warehouse}',
        's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
        's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
    )"""
    test {
        sql "select * from test_deltalake_errors.delta_golden.spark_variant_checkpoint"
        exception "Unsupported Delta Lake reader features [variantType-preview]"
    }
    test {
        sql "select * from test_deltalake_errors.delta_golden.collations_table"
        exception "Unsupported Delta Lake type: string collate UTF8_LCASE"
    }
    test {
        sql "select * from test_deltalake_errors.delta_golden.no_delta_log_folder"
        exception "Table [no_delta_log_folder] does not exist"
    }
    test {
        sql "select * from test_deltalake_errors.delta_golden.does_not_exist"
        exception "Table [does_not_exist] does not exist"
    }
}
