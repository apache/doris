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

package org.apache.doris.paimon;

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ScanPredicate;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.hive.HiveCatalogOptions;
import org.apache.paimon.hive.mapred.PaimonInputSplit;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;


public class PaimonJniScanner extends JniScanner {
    private static final Logger LOG = Logger.getLogger(PaimonJniScanner.class);

    private final String metastoreUris;
    private final String warehouse;
    private final String dbName;
    private final String tblName;
    private final String[] ids;
    private final long splitAddress;
    private final int lengthByte;
    private PaimonInputSplit paimonInputSplit;
    private Table table;
    private RecordReader<InternalRow> reader;
    private final PaimonColumnValue columnValue = new PaimonColumnValue();

    public PaimonJniScanner(int batchSize, Map<String, String> params) {
        metastoreUris = params.get("hive.metastore.uris");
        warehouse = params.get("warehouse");
        splitAddress = Long.parseLong(params.get("split_byte"));
        lengthByte = Integer.parseInt(params.get("length_byte"));
        LOG.info("splitAddress:" + splitAddress);
        LOG.info("lengthByte:" + lengthByte);
        dbName = params.get("db_name");
        tblName = params.get("table_name");
        String[] requiredFields = params.get("required_fields").split(",");
        String[] types = params.get("columns_types").split(",");
        ids = params.get("columns_id").split(",");
        ColumnType[] columnTypes = new ColumnType[types.length];
        for (int i = 0; i < types.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], types[i]);
        }
        ScanPredicate[] predicates = new ScanPredicate[0];
        if (params.containsKey("push_down_predicates")) {
            long predicatesAddress = Long.parseLong(params.get("push_down_predicates"));
            if (predicatesAddress != 0) {
                predicates = ScanPredicate.parseScanPredicates(predicatesAddress, columnTypes);
                LOG.info("MockJniScanner gets pushed-down predicates:  " + ScanPredicate.dump(predicates));
            }
        }
        initTableInfo(columnTypes, requiredFields, predicates, batchSize);
    }

    @Override
    public void open() throws IOException {
        getCatalog();
        // deserialize it into split
        byte[] splitByte = new byte[lengthByte];
        OffHeap.copyMemory(null, splitAddress, splitByte, OffHeap.BYTE_ARRAY_OFFSET, lengthByte);
        ByteArrayInputStream bais = new ByteArrayInputStream(splitByte);
        DataInputStream input = new DataInputStream(bais);
        try {
            paimonInputSplit.readFields(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ReadBuilder readBuilder = table.newReadBuilder()
                                    .withProjection(Arrays.stream(ids).mapToInt(Integer::parseInt).toArray());
        TableRead read = readBuilder.newRead();
        reader = read.createReader(paimonInputSplit.split());
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    protected int getNext() throws IOException {
        int rows = 0;
        try {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                Object record;
                while ((record = batch.next()) != null) {
                    columnValue.setOffsetRow((ColumnarRow) record);
                    for (int i = 0; i < ids.length; i++) {
                        columnValue.setIdx(i);
                        appendData(i, columnValue);
                    }
                    rows++;
                }
                batch.releaseBatch();
            }
        } catch (IOException e) {
            LOG.warn("failed to getNext columnValue ", e);
            throw new RuntimeException(e);
        }
        return rows;
    }

    private Catalog create(CatalogContext context) throws IOException {
        Path warehousePath = new Path(context.options().get(CatalogOptions.WAREHOUSE));
        FileIO fileIO;
        fileIO = FileIO.get(warehousePath, context);
        String uri = context.options().get(CatalogOptions.URI);
        String hiveConfDir = context.options().get(HiveCatalogOptions.HIVE_CONF_DIR);
        String hadoopConfDir = context.options().get(HiveCatalogOptions.HADOOP_CONF_DIR);
        HiveConf hiveConf = HiveCatalog.createHiveConf(hiveConfDir, hadoopConfDir);

        // always using user-set parameters overwrite hive-site.xml parameters
        context.options().toMap().forEach(hiveConf::set);
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, uri);
        // set the warehouse location to the hiveConf
        hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, context.options().get(CatalogOptions.WAREHOUSE));

        String clientClassName = context.options().get(METASTORE_CLIENT_CLASS);

        return new HiveCatalog(fileIO, hiveConf, clientClassName, context.options().toMap());
    }

    private void getCatalog() {
        paimonInputSplit = new PaimonInputSplit();
        Options options = new Options();
        options.set("warehouse", warehouse);
        // Currently, only supports hive
        options.set("metastore", "hive");
        options.set("uri", metastoreUris);
        CatalogContext context = CatalogContext.create(options);
        try {
            Catalog catalog = create(context);
            table = catalog.getTable(Identifier.create(dbName, tblName));
        } catch (IOException | Catalog.TableNotExistException e) {
            LOG.warn("failed to create paimon external catalog ", e);
            throw new RuntimeException(e);
        }
    }

    private static final ConfigOption<String> METASTORE_CLIENT_CLASS =
            ConfigOptions.key("metastore.client.class")
            .stringType()
            .defaultValue("org.apache.hadoop.hive.metastore.HiveMetaStoreClient")
            .withDescription(
                "Class name of Hive metastore client.\n"
                    + "NOTE: This class must directly implements "
                    + "org.apache.hadoop.hive.metastore.IMetaStoreClient.");
}
