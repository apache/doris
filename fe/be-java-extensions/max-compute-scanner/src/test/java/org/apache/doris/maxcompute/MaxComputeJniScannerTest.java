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

package org.apache.doris.maxcompute;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrowRecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.type.TypeInfoFactory;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MaxComputeJniScannerTest {

    @Mocked
    private TableTunnel.DownloadSession session;
    private Map<String, String> paramsMc = new HashMap<String, String>() {
        {
            put("region", "cn-beijing");
            put("project", "test_pj");
            put("table", "test_tb");
            put("access_key", "ak");
            put("secret_key", "sk");
            put("start_offset", "0");
            put("split_size", "128");
            put("partition_spec", "p1=2022-06");
            put("required_fields", "boolean,tinyint,smallint,int,bigint,float,double,"
                    + "date,timestamp,char,varchar,string,decimalv2,decimal64,"
                    + "decimal18,timestamp4");
            put("columns_types", "boolean#tinyint#smallint#int#bigint#float#double#"
                    + "date#timestamp#char(10)#varchar(10)#string#decimalv2(12,4)#decimal64(10,3)#"
                    + "decimal(18,5)#timestamp(4)");
        }
    };
    private MaxComputeJniScanner scanner = new MaxComputeJniScanner(32, paramsMc);

    @BeforeEach
    public void init() {
        new MockUp<MaxComputeJniScanner>(MaxComputeJniScanner.class) {
            @Mock
            public TableSchema getSchema() {
                return getTestSchema();
            }
        };
        new MockUp<MaxComputeTableScan>(MaxComputeTableScan.class) {
            @Mock
            public TableSchema getSchema() {
                return getTestSchema();
            }

            @Mock
            public TableTunnel.DownloadSession openDownLoadSession() throws IOException {
                return session;
            }

            @Mock
            public TableTunnel.DownloadSession openDownLoadSession(PartitionSpec partitionSpec) throws IOException {
                return session;
            }
        };
        new MockUp<TableTunnel.DownloadSession>(TableTunnel.DownloadSession.class) {
            @Mock
            public TableSchema getSchema() {
                return getTestSchema();
            }

            @Mock
            public long getRecordCount() {
                return 10;
            }

            @Mock
            public ArrowRecordReader openArrowRecordReader(long start, long count, List<Column> columns,
                                                           BufferAllocator allocator)
                    throws TunnelException, IOException {
                return null;
            }
        };
    }

    private TableSchema getTestSchema() {
        TableSchema schema = new TableSchema();
        schema.addColumn(new Column("boolean", TypeInfoFactory.BOOLEAN));
        schema.addColumn(new Column("bigint", TypeInfoFactory.BIGINT));
        schema.addPartitionColumn(new Column("date", TypeInfoFactory.DATE));
        schema.addPartitionColumn(new Column("tinyint", TypeInfoFactory.TINYINT));
        schema.addPartitionColumn(new Column("smallint", TypeInfoFactory.SMALLINT));
        schema.addPartitionColumn(new Column("int", TypeInfoFactory.INT));
        schema.addPartitionColumn(new Column("timestamp", TypeInfoFactory.TIMESTAMP));
        schema.addPartitionColumn(new Column("char", TypeInfoFactory.getCharTypeInfo(10)));
        schema.addPartitionColumn(new Column("varchar", TypeInfoFactory.getVarcharTypeInfo(10)));
        schema.addPartitionColumn(new Column("string", TypeInfoFactory.STRING));
        schema.addPartitionColumn(new Column("float", TypeInfoFactory.FLOAT));
        schema.addPartitionColumn(new Column("double", TypeInfoFactory.DOUBLE));
        schema.addPartitionColumn(new Column("decimalv2",
                TypeInfoFactory.getDecimalTypeInfo(12, 4)));
        schema.addPartitionColumn(new Column("decimal64",
                TypeInfoFactory.getDecimalTypeInfo(10, 3)));
        schema.addPartitionColumn(new Column("decimal18",
                TypeInfoFactory.getDecimalTypeInfo(18, 5)));
        schema.addPartitionColumn(new Column("timestamp4", TypeInfoFactory.TIMESTAMP));
        return schema;
    }

    @Test
    public void testMaxComputeJniScanner() throws IOException {
        scanner.open();
        scanner.getNext();
        scanner.close();
    }

    @Test
    public void testMaxComputeJniScannerErr() {
        try {
            new MockUp<TableTunnel.DownloadSession>(TableTunnel.DownloadSession.class) {
                @Mock
                public ArrowRecordReader openArrowRecordReader(long start, long count, List<Column> columns,
                                                               BufferAllocator allocator)
                        throws TunnelException, IOException {
                    throw new TunnelException("TableModified");
                }
            };
            scanner.open();
            scanner.getNext();
            scanner.close();
        } catch (IOException e) {
            Assertions.assertTrue(e.getCause() instanceof TunnelException);
            Assertions.assertEquals(((TunnelException) e.getCause()).getErrorMsg(), "TableModified");
        }
    }
}
