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

package org.apache.doris.catalog.external;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.cassandra.CassandraExternalCatalog;
import org.apache.doris.thrift.TCassandraTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.type.DefaultListType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.datastax.oss.driver.internal.core.type.DefaultSetType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.google.common.collect.Lists;
import org.apache.paimon.types.DecimalType;

import java.util.List;
import java.util.Map;

public class CassandraExternalTable extends ExternalTable {

    private TableMetadata table;

    public CassandraExternalTable(long id, String name, String dbName, CassandraExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.CASSANDRA_EXTERNAL_TABLE);
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            table = ((CassandraExternalCatalog) catalog).getTable(dbName, name);
        }
    }

    @Override
    public List<Column> initSchema() {
        makeSureInitialized();
        Map<CqlIdentifier, ColumnMetadata> columns = table.getColumns();
        List<Column> result = Lists.newArrayListWithCapacity(columns.size());
        columns.forEach((cqlIdentifier, columnMetadata) -> {
            result.add(new Column(columnMetadata.getName().asInternal(),
                    cassandraTypeToDorisType(columnMetadata.getType())));
        });

        return result;

    }

    private Type cassandraTypeToDorisType(DataType dataType) {
        switch (dataType.getProtocolCode()) {
            case ProtocolConstants.DataType.BOOLEAN:
                return Type.BOOLEAN;
            case ProtocolConstants.DataType.INT:
                return Type.INT;
            case ProtocolConstants.DataType.COUNTER:
            case ProtocolConstants.DataType.BIGINT:
                return Type.BIGINT;
            case ProtocolConstants.DataType.FLOAT:
                return Type.FLOAT;
            case ProtocolConstants.DataType.DOUBLE:
                return Type.DOUBLE;
            case ProtocolConstants.DataType.TINYINT:
                return Type.TINYINT;
            case ProtocolConstants.DataType.SMALLINT:
                return Type.SMALLINT;
            case ProtocolConstants.DataType.VARCHAR:
            case ProtocolConstants.DataType.VARINT:
            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.UUID:
            case ProtocolConstants.DataType.INET:
            case ProtocolConstants.DataType.TIMEUUID:
                return Type.STRING;
            case ProtocolConstants.DataType.DECIMAL:
                DecimalType decimal = (DecimalType) dataType;
                return ScalarType.createDecimalV3Type(decimal.getPrecision(), decimal.getScale());
            case ProtocolConstants.DataType.DATE:
                return ScalarType.createDateV2Type();
            case ProtocolConstants.DataType.TIMESTAMP:
                return ScalarType.createDatetimeV2Type(3);
            case ProtocolConstants.DataType.TIME:
                return Type.TIME;
            case ProtocolConstants.DataType.MAP:
                DefaultMapType mapType = (DefaultMapType) dataType;
                return new MapType(cassandraTypeToDorisType(mapType.getKeyType()),
                        cassandraTypeToDorisType(mapType.getKeyType()));
            case ProtocolConstants.DataType.LIST:
                DefaultListType listType = (DefaultListType) dataType;
                Type listInnerType = cassandraTypeToDorisType(listType.getElementType());
                return ArrayType.create(listInnerType, true);
            case ProtocolConstants.DataType.SET:
                DefaultSetType setType = (DefaultSetType) dataType;
                Type setInnerType = cassandraTypeToDorisType(setType.getElementType());
                return ArrayType.create(setInnerType, true);
            case ProtocolConstants.DataType.BLOB:
            case ProtocolConstants.DataType.CUSTOM:
                return Type.UNSUPPORTED;
            default:
                throw new IllegalArgumentException("Cannot transform unknown type: " + dataType.getProtocolCode());
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        TCassandraTable tCassandraTable = new TCassandraTable();
        tCassandraTable.setContactPoints(((CassandraExternalCatalog) catalog).getContactPoints());
        tCassandraTable.setKeyspace(dbName);
        tCassandraTable.setTable(name);
        tCassandraTable.setUsername(((CassandraExternalCatalog) catalog).getUserName());
        tCassandraTable.setPassword(((CassandraExternalCatalog) catalog).getPassword());
        tCassandraTable.setDatacenter(((CassandraExternalCatalog) catalog).getDatacenter());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.CASSANDRA_TABLE, schema.size(), 0,
                getName(), dbName);
        tTableDescriptor.setCassandraTable(tCassandraTable);
        return tTableDescriptor;

    }

    @Override
    public String getMysqlType() {
        return "BASE TABLE";
    }

}
