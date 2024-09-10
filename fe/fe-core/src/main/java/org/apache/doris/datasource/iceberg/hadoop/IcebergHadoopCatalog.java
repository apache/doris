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

package org.apache.doris.datasource.iceberg.hadoop;

import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.LockManagers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IcebergHadoopCatalog extends HadoopCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergHadoopCatalog.class);
    private String warehouseLocation;
    private DFSFileSystem fs;
    private CloseableGroup closeableGroup;
    private LockManager lockManager;
    protected FileIO fileIO;
    protected String uid;
    private static final Joiner SLASH = Joiner.on("/");

    public void initialize(String name, Map<String, String> properties) {
        super.initialize(name, properties);
        String inputWarehouseLocation = properties.get("warehouse");
        Preconditions.checkArgument(inputWarehouseLocation != null && inputWarehouseLocation.length() > 0,
                "Cannot initialize HadoopCatalog because warehousePath must not be null or empty");
        this.warehouseLocation = LocationUtil.stripTrailingSlash(inputWarehouseLocation);
        this.fs = new DFSFileSystem(properties);
        this.fileIO = initializeFileIO(properties, getConf());
        this.lockManager = LockManagers.from(properties);
        this.closeableGroup = new CloseableGroup();
        this.closeableGroup.addCloseable(this.lockManager);
        this.closeableGroup.setSuppressCloseFailure(true);
    }

    protected FileIO initializeFileIO(Map<String, String> properties, Configuration hadoopConf) {
        String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
        if (fileIOImpl == null) {
            /* when use the S3FileIO, we need some custom configurations,
             * so HadoopFileIO is used in the superclass by default
             * we can add better implementations to derived class just like the implementation in DLFCatalog.
             */
            FileIO io = new IcebergHadoopFileIO(hadoopConf, this.fs);
            io.initialize(properties);
            return io;
        } else {
            return CatalogUtil.loadFileIO(fileIOImpl, properties, hadoopConf);
        }
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier identifier) {
        return new IcebergHadoopTableOperations(new Path(this.defaultWarehouseLocation(identifier)),
                this.fileIO, getConf(), this.lockManager, this.fs);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        String tableName = tableIdentifier.name();
        StringBuilder sb = new StringBuilder();
        sb.append(this.warehouseLocation).append('/');
        String[] levels = tableIdentifier.namespace().levels();
        for (String level : levels) {
            sb.append(level).append('/');
        }
        sb.append(tableName);
        return sb.toString();
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> meta) {
        Preconditions.checkArgument(!namespace.isEmpty(),
                "Cannot create namespace with invalid name: %s", namespace);
        if (!meta.isEmpty()) {
            throw new UnsupportedOperationException("Cannot create namespace " + namespace
                    + ": metadata is not supported");
        } else {
            Path nsPath = new Path(this.warehouseLocation, SLASH.join(namespace.levels()));
            if (this.isNamespace(nsPath)) {
                throw new AlreadyExistsException("Namespace already exists: %s", namespace);
            } else {
                try {
                    this.fs.mkdirs(nsPath);
                } catch (IOException e) {
                    throw new RuntimeIOException(e, "Create namespace failed: %s", namespace);
                }
            }
        }
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) {
        Path nsPath = namespace.isEmpty()
                ? new Path(warehouseLocation)
                : new Path(warehouseLocation, SLASH.join(namespace.levels()));
        if (!isNamespace(nsPath)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }

        try {
            // using the iterator listing allows for paged downloads
            // from HDFS and prefetching from object storage.
            List<Namespace> namespaces = new ArrayList<>();
            RemoteIterator<FileStatus> it = fs.listStatusIterator(nsPath);
            while (it.hasNext()) {
                Path path = it.next().getPath();
                if (isNamespace(path)) {
                    namespaces.add(append(namespace, path.getName()));
                }
            }
            return namespaces;
        } catch (IOException ioe) {
            throw new RuntimeIOException(ioe, "Failed to list namespace under: %s", namespace);
        }
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
        Path nsPath = new Path(this.warehouseLocation, SLASH.join(namespace.levels()));
        if (this.isNamespace(nsPath) && !namespace.isEmpty()) {
            return ImmutableMap.of("location", nsPath.toString());
        } else {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }
    }

    @Override
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        Path nsPath = new Path(this.warehouseLocation, SLASH.join(namespace.levels()));
        if (this.isNamespace(nsPath) && !namespace.isEmpty()) {
            try {
                if (this.fs.listStatusIterator(nsPath).hasNext()) {
                    throw new NamespaceNotEmptyException("Namespace %s is not empty.", new Object[]{namespace});
                } else {
                    return this.fs.delete(nsPath, false);
                }
            } catch (IOException e) {
                throw new RuntimeIOException(e, "Namespace delete failed: %s", new Object[]{namespace});
            }
        } else {
            return false;
        }
    }

    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        if (!this.isValidIdentifier(identifier)) {
            throw new NoSuchTableException("Invalid identifier: %s", identifier);
        } else {
            Path tablePath = new Path(this.defaultWarehouseLocation(identifier));
            TableOperations ops = this.newTableOps(identifier);
            TableMetadata lastMetadata = ops.current();
            try {
                if (lastMetadata == null) {
                    LOG.debug("Not an iceberg table: {}", identifier);
                    return false;
                } else {
                    if (purge) {
                        CatalogUtil.dropTableData(ops.io(), lastMetadata);
                    }
                    return this.fs.delete(tablePath, true);
                }
            } catch (IOException e) {
                throw new RuntimeIOException(e, "Failed to delete file: %s", tablePath);
            }
        }
    }

    private Namespace append(Namespace ns, String name) {
        String[] levels = Arrays.copyOfRange(ns.levels(), 0, ns.levels().length + 1);
        levels[ns.levels().length] = name;
        return Namespace.of(levels);
    }

    private boolean isNamespace(Path path) {
        return isDirectory(path) && !isTableDir(path);
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        Preconditions.checkArgument(namespace.levels().length >= 1,
                "Missing database in table identifier: %s", namespace);
        Path nsPath = new Path(this.warehouseLocation, Joiner.on("/").join(namespace.levels()));
        Set<TableIdentifier> tblIdents = Sets.newHashSet();

        try {
            if (!isDirectory(nsPath)) {
                throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
            }
            RemoteIterator<FileStatus> it = this.fs.listStatusIterator(nsPath);
            while (it.hasNext()) {
                FileStatus status = it.next();
                if (status.isDirectory()) {
                    Path path = status.getPath();
                    if (isTableDir(path)) {
                        TableIdentifier tblIdent = TableIdentifier.of(namespace, path.getName());
                        tblIdents.add(tblIdent);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to list tables under: %s", namespace);
        }

        return Lists.newArrayList(tblIdents);
    }

    private boolean isTableDir(Path path) {
        Path metadataPath = new Path(path, "metadata");
        PathFilter tableFilter = (filterPath) -> filterPath.getName().endsWith(".metadata.json");
        try {
            return fs.listStatus(metadataPath, tableFilter).length >= 1;
        } catch (FileNotFoundException f) {
            return false;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean isDirectory(Path path) {
        try {
            return fs.getFileStatus(path).isDirectory();
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            LOG.warn("Unable to list directory {}", path, e);
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        this.closeableGroup.close();
    }

    @Override
    public void setConf(Configuration configuration) {
        super.setConf(configuration);
    }

    @Override
    public Configuration getConf() {
        Configuration conf = super.getConf();
        if (conf == null) {
            return new HdfsConfiguration();
        }
        return conf;
    }
}
