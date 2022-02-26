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

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.external.iceberg.IcebergCatalog;
import org.apache.doris.external.iceberg.IcebergCatalogMgr;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TIcebergTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * External Iceberg table
 */
public class IcebergTable extends Table {
    private static final Logger LOG = LogManager.getLogger(IcebergTable.class);

    // remote Iceberg database name
    private String icebergDb;
    // remote Iceberg table name
    private String icebergTbl;
    // remote Iceberg table location
    private String location;
    // Iceberg table file format
    private String fileFormat;
    // Iceberg storage type
    private StorageBackend.StorageType storageType;
    // Iceberg remote host uri
    private String hostUri;
    // location analyze flag
    private boolean isAnalyzed = false;
    private Map<String, String> icebergProperties = Maps.newHashMap();

    private org.apache.iceberg.Table icebergTable;

    private final byte[] loadLock = new byte[0];
    private final AtomicBoolean isLoaded = new AtomicBoolean(false);

    public IcebergTable() {
        super(TableType.ICEBERG);
    }

    public IcebergTable(long id, String tableName, List<Column> fullSchema, IcebergProperty icebergProperty,
                        org.apache.iceberg.Table icebergTable) {
        super(id, tableName, TableType.ICEBERG, fullSchema);
        this.icebergDb = icebergProperty.getDatabase();
        this.icebergTbl = icebergProperty.getTable();

        icebergProperties.put(IcebergProperty.ICEBERG_HIVE_METASTORE_URIS,
                icebergProperty.getHiveMetastoreUris());
        icebergProperties.put(IcebergProperty.ICEBERG_CATALOG_TYPE,
                icebergProperty.getCatalogType());
        this.icebergTable = icebergTable;
    }

    public String getIcebergDbTable() {
        return String.format("%s.%s", icebergDb, icebergTbl);
    }

    public String getIcebergDb() {
        return icebergDb;
    }

    public String getIcebergTbl() {
        return icebergTbl;
    }

    public Map<String, String> getIcebergProperties() {
        return icebergProperties;
    }

    private void getLocation() throws UserException {
        if (Strings.isNullOrEmpty(location)) {
            try {
                getTable();
            } catch (Exception e) {
                throw new UserException("Failed to get table: " + name + ",error: " + e.getMessage());
            }
            location = icebergTable.location();
        }
        analyzeLocation();
    }

    private void analyzeLocation() throws UserException {
        if (isAnalyzed) {
            return;
        }
        String[] strings = StringUtils.split(location, "/");

        // analyze storage type
        String storagePrefix = strings[0].split(":")[0];
        if (storagePrefix.equalsIgnoreCase("s3")) {
            this.storageType = StorageBackend.StorageType.S3;
        } else if (storagePrefix.equalsIgnoreCase("hdfs")) {
            this.storageType = StorageBackend.StorageType.HDFS;
        } else {
            throw new UserException("Not supported storage type: " + storagePrefix);
        }

        // analyze host uri
        // eg: hdfs://host:port
        //     s3://host:port
        String host = strings[1];
        this.hostUri = storagePrefix + "://" + host;
        this.isAnalyzed = true;
    }

    public String getHostUri() throws UserException {
        if (!isAnalyzed) {
            getLocation();
        }
        return hostUri;
    }

    public StorageBackend.StorageType getStorageType() throws UserException {
        if (!isAnalyzed) {
            getLocation();
        }
        return storageType;
    }

    public String getFileFormat() throws UserException {
        if (Strings.isNullOrEmpty(fileFormat)) {
            try {
                getTable();
            } catch (Exception e) {
                throw new UserException("Failed to get table: " + name + ",error: " + e.getMessage());
            }
            fileFormat = icebergTable.properties().get(TableProperties.DEFAULT_FILE_FORMAT);
        }
        return fileFormat;
    }

    // get the iceberg table instance, if table is not loaded, load it.
    private org.apache.iceberg.Table getTable() throws Exception {
        if (isLoaded.get()) {
            Preconditions.checkNotNull(icebergTable);
            return icebergTable;
        }
        synchronized (loadLock) {
            if (icebergTable != null) {
                return icebergTable;
            }

            IcebergProperty icebergProperty = getIcebergProperty();
            IcebergCatalog icebergCatalog = IcebergCatalogMgr.getCatalog(icebergProperty);
            try {
                this.icebergTable = icebergCatalog.loadTable(TableIdentifier.of(icebergDb, icebergTbl));
                LOG.info("finished to load iceberg table: {}", name);
            } catch (Exception e) {
                LOG.warn("failed to load iceberg table {} from {}", name, icebergProperty.getHiveMetastoreUris(), e);
                throw e;
            }

            isLoaded.set(true);
            return icebergTable;
        }
    }

    private IcebergProperty getIcebergProperty() {
        Map<String, String> properties = Maps.newHashMap(icebergProperties);
        properties.put(IcebergProperty.ICEBERG_DATABASE, icebergDb);
        properties.put(IcebergProperty.ICEBERG_TABLE, icebergTbl);
        return new IcebergProperty(properties);
    }

    /**
     * Get iceberg data file by file system table location and iceberg predicates
     * @throws Exception
     */
    public List<TBrokerFileStatus> getIcebergDataFiles(List<Expression> predicates) throws Exception {
        org.apache.iceberg.Table table = getTable();
        TableScan scan = table.newScan();
        for (Expression predicate : predicates) {
            scan = scan.filter(predicate);
        }
        List<TBrokerFileStatus> relatedFiles = Lists.newArrayList();
        for (FileScanTask task : scan.planFiles()) {
            Path path = Paths.get(task.file().path().toString());
            String relativePath = "/" + path.subpath(2, path.getNameCount());
            relatedFiles.add(new TBrokerFileStatus(relativePath, false, task.file().fileSizeInBytes(), false));
        }
        return relatedFiles;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Text.writeString(out, icebergDb);
        Text.writeString(out, icebergTbl);

        out.writeInt(icebergProperties.size());
        for (Map.Entry<String, String> entry : icebergProperties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        icebergDb = Text.readString(in);
        icebergTbl = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            icebergProperties.put(key, value);
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        TIcebergTable tIcebergTable = new TIcebergTable(getIcebergDb(), getIcebergTbl(), getIcebergProperties());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ICEBERG_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setIcebergTable(tIcebergTable);
        return tTableDescriptor;
    }
}
