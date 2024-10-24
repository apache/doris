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

package org.apache.doris.catalog;

import org.apache.doris.analysis.SchemaTableType;
import org.apache.doris.common.SystemIdGenerator;
import org.apache.doris.thrift.TSchemaTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * Doris's representation of MySQL mysql table metadata.
 */
public class MysqlDBTable extends SchemaTable {

    public static Map<String, Table> TABLE_MAP = ImmutableMap.<String, Table>builder().put("user",
                    new MysqlDBTable(SystemIdGenerator.getNextId(), "user", TableType.SCHEMA,
                            builder().column("Host", ScalarType.createCharType(255))
                                    .column("User", ScalarType.createCharType(32))
                                    .column("Node_priv", ScalarType.createCharType(1))
                                    .column("Admin_priv", ScalarType.createCharType(1))
                                    .column("Grant_priv", ScalarType.createCharType(1))
                                    .column("Select_priv", ScalarType.createCharType(1))
                                    .column("Load_priv", ScalarType.createCharType(1))
                                    .column("Alter_priv", ScalarType.createCharType(1))
                                    .column("Create_priv", ScalarType.createCharType(1))
                                    .column("Drop_priv", ScalarType.createCharType(1))
                                    .column("Usage_priv", ScalarType.createCharType(1))
                                    .column("Show_view_priv", ScalarType.createCharType(1))
                                    .column("Cluster_usage_priv", ScalarType.createCharType(1))
                                    .column("Stage_usage_priv", ScalarType.createCharType(1))
                                    .column("ssl_type", ScalarType.createCharType(9))
                                    .column("ssl_cipher", ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH))
                                    .column("x509_issuer", ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH))
                                    .column("x509_subject", ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH))
                                    .column("max_questions", Type.BIGINT)
                                    .column("max_updates", Type.BIGINT)
                                    .column("max_connections", Type.BIGINT)
                                    .column("max_user_connections", Type.BIGINT)
                                    .column("plugin", ScalarType.createCharType(64))
                                    .column("authentication_string",
                                            ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH))
                                    .column("password_policy.expiration_seconds", ScalarType.createVarcharType(32))
                                    .column("password_policy.password_creation_time", ScalarType.createVarcharType(32))
                                    .column("password_policy.history_num", ScalarType.createVarcharType(32))
                                    .column("password_policy.history_passwords",
                                            ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH))
                                    .column("password_policy.num_failed_login", ScalarType.createVarcharType(32))
                                    .column("password_policy.password_lock_seconds", ScalarType.createVarcharType(32))
                                    .column("password_policy.failed_login_counter", ScalarType.createVarcharType(32))
                                    .column("password_policy.lock_time", ScalarType.createVarcharType(32))
                                    .build()))
            .put("procs_priv",
                    new MysqlDBTable(SystemIdGenerator.getNextId(), "procs_priv", TableType.SCHEMA,
                            builder().column("Host", ScalarType.createCharType(60))
                                    .column("Db", ScalarType.createCharType(64))
                                    .column("User", ScalarType.createCharType(32))
                                    .column("Routine_name", ScalarType.createCharType(64))
                                    .column("Routine_type", ScalarType.createCharType(9))
                                    .column("Grantor", ScalarType.createCharType(93))
                                    .column("Proc_priv", ScalarType.createCharType(16))
                                    .column("Timestamp", ScalarType.createCharType(1))
                                    .build()))
            .build();

    public MysqlDBTable(long id, String tableName, TableType type, List<Column> fullSchema) {
        super(id, tableName, type, fullSchema);
    }

    @Override
    public TTableDescriptor toThrift() {
        TSchemaTable tSchemaTable = new TSchemaTable(SchemaTableType.getThriftType(this.name));
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.SCHEMA_TABLE,
                TABLE_MAP.get(this.name).getBaseSchema().size(), 0, this.name, "");
        tTableDescriptor.setSchemaTable(tSchemaTable);
        return tTableDescriptor;
    }
}
