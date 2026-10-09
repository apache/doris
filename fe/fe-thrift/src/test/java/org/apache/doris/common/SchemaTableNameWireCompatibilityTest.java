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

package org.apache.doris.common;

import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TSchemaTableName;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

public class SchemaTableNameWireCompatibilityTest {
    @ParameterizedTest
    @CsvSource({
            // IDs 1-12 are shared with branch-4.0; 13/14 are fixed by branch-4.1/branch-4.2.
            "METADATA_TABLE, 1",
            "ACTIVE_QUERIES, 2",
            "WORKLOAD_GROUPS, 3",
            "ROUTINES_INFO, 4",
            "WORKLOAD_SCHEDULE_POLICY, 5",
            "TABLE_OPTIONS, 6",
            "WORKLOAD_GROUP_PRIVILEGES, 7",
            "TABLE_PROPERTIES, 8",
            "CATALOG_META_CACHE_STATS, 9",
            "PARTITIONS, 10",
            "VIEW_DEPENDENCY, 11",
            "SQL_BLOCK_RULE_STATUS, 12",
            "AUTHENTICATION_INTEGRATIONS, 13",
            "ROLE_MAPPINGS, 14",
            "TABLE_STREAMS, 15",
            "TABLE_STREAM_CONSUMPTION, 16",
            "DATABASE_PROPERTIES, 17",
            "EXTENSIONS, 18",
            "TSO_STATUS, 19",
            "STATISTICS, 20",
            "KEY_COLUMN_USAGE, 21",
            "TABLE_CONSTRAINTS, 22"
    })
    public void testSchemaTableRequestWireIds(TSchemaTableName tableName, int wireId) throws Exception {
        for (TProtocolFactory factory : List.of(new TBinaryProtocol.Factory(), new TCompactProtocol.Factory())) {
            // Decode a numeric request independently of the generated enum's current values.
            TFetchSchemaTableDataRequest request = readRequest(wireId, factory);
            Assertions.assertEquals(tableName, request.getSchemaTableName());

            TMemoryBuffer output = new TMemoryBuffer(128);
            new TFetchSchemaTableDataRequest().setSchemaTableName(tableName).write(factory.getProtocol(output));
            TProtocol reader = factory.getProtocol(output);
            reader.readStructBegin();
            TField field = reader.readFieldBegin();
            Assertions.assertEquals(2, field.id);
            Assertions.assertEquals(TType.I32, field.type);
            Assertions.assertEquals(wireId, reader.readI32());
            reader.readFieldEnd();
            Assertions.assertEquals(TType.STOP, reader.readFieldBegin().type);
            reader.readStructEnd();
        }
    }

    private TFetchSchemaTableDataRequest readRequest(int wireId, TProtocolFactory factory) throws Exception {
        TMemoryBuffer input = new TMemoryBuffer(128);
        TProtocol writer = factory.getProtocol(input);
        writer.writeStructBegin(new TStruct("TFetchSchemaTableDataRequest"));
        writer.writeFieldBegin(new TField("schema_table_name", TType.I32, (short) 2));
        writer.writeI32(wireId);
        writer.writeFieldEnd();
        writer.writeFieldStop();
        writer.writeStructEnd();
        TFetchSchemaTableDataRequest request = new TFetchSchemaTableDataRequest();
        request.read(factory.getProtocol(input));
        return request;
    }
}
