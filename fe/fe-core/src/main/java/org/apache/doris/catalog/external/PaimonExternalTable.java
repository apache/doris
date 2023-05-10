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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.hive.mapred.PaimonInputSplit;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.utils.OffsetRow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class PaimonExternalTable extends ExternalTable {

    private static final Logger LOG = LogManager.getLogger(PaimonExternalTable.class);

    public static final int PAIMON_DATETIME_SCALE_MS = 3;
    private Table originTable = null;

    public PaimonExternalTable(long id, String name, String dbName, PaimonExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.PAIMON_EXTERNAL_TABLE);
    }

    public String getPaimonCatalogType() {
        return ((PaimonExternalCatalog) catalog).getPaimonCatalogType();
    }

    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    public Table getOriginTable() {
        if (originTable == null) {
            originTable = ((PaimonExternalCatalog) catalog).getPaimonTable(dbName, name);
        }
        return originTable;
    }

    @Override
    public List<Column> initSchema() {
        Table table = getOriginTable();
        TableSchema schema = ((AbstractFileStoreTable) table).schema();
        // test Whether the data can be read normally
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        TableRead read = readBuilder.newRead();
        try {
            RecordReader<InternalRow> reader = read.createReader(splits);
            RecordReader.RecordIterator batch;
            while ((batch = reader.readBatch()) != null) {
                Object record;
                while ((record = batch.next()) != null) {
                    if (record instanceof OffsetRow) {
                        OffsetRow record1 = (OffsetRow) record;
                        LOG.warn(record1.getInt(0) + "   :0000");
                        LOG.warn(record1.getString(1) + "   :1111");
                        LOG.warn("row.getFieldCount(): " + record1.getFieldCount());
                        LOG.warn("row.getRowKind(): " + record1.getRowKind());
                    }
                }
                batch.releaseBatch();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            LOG.warn("123455678");
        }

        // test Is it possible to split and then read
        List<Split> splities = readBuilder.newScan().plan().splits();
        PaimonInputSplit split = new PaimonInputSplit(
                "tempDir.toString()",
                (DataSplit) splities.get(0)
               );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(baos);
        try {
            split.write(output);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] bytes = baos.toByteArray();

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream input = new DataInputStream(bais);
        PaimonInputSplit actual = new PaimonInputSplit();
        try {
            actual.readFields(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        TableRead read1 = readBuilder.newRead();
        try {
            RecordReader<InternalRow> reader = read1.createReader(actual.split());
            RecordReader.RecordIterator batch;
            while ((batch = reader.readBatch()) != null) {
                Object record;
                while ((record = batch.next()) != null) {
                    if (record instanceof OffsetRow) {
                        OffsetRow record1 = (OffsetRow) record;
                        LOG.warn(record1.getInt(0) + "   :0000");
                        LOG.warn(record1.getString(1) + "   :1111");
                        LOG.warn("row.getFieldCount(): " + record1.getFieldCount());
                        LOG.warn("row.getRowKind(): " + record1.getRowKind());
                    }
                }
                batch.releaseBatch();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            LOG.warn("456789");
        }
        // test end
        List<DataField> columns = schema.fields();
        List<Column> tmpSchema = Lists.newArrayListWithCapacity(columns.size());
        for (DataField field : columns) {
            tmpSchema.add(new Column(field.name(),
                    paimonTypeToDorisType(field.type()), true, null, true, field.description(), true,
                    field.id()));
        }
        return tmpSchema;
    }

    private Type paimonPrimitiveTypeToDorisType(org.apache.paimon.types.DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case INTEGER:
                return Type.INT;
            case BIGINT:
                return Type.BIGINT;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case VARCHAR:
            case BINARY:
            case CHAR:
                return Type.STRING;
            case DECIMAL:
                DecimalType decimal = (DecimalType) dataType;
                return ScalarType.createDecimalV3Type(decimal.getPrecision(), decimal.getScale());
            case DATE:
                return ScalarType.createDateV2Type();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ScalarType.createDatetimeV2Type(PAIMON_DATETIME_SCALE_MS);
            case TIME_WITHOUT_TIME_ZONE:
                return Type.UNSUPPORTED;
            default:
                throw new IllegalArgumentException("Cannot transform unknown type: " + dataType.getTypeRoot());
        }
    }

    protected Type paimonTypeToDorisType(org.apache.paimon.types.DataType type) {
        return paimonPrimitiveTypeToDorisType(type);
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        if (getPaimonCatalogType().equals("hms")) {
            THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), dbName);
            tTableDescriptor.setHiveTable(tHiveTable);
            return tTableDescriptor;
        } else {
            throw new IllegalArgumentException("Currently only supports hms catalog,not support :"
                + getPaimonCatalogType());
        }
    }
}
