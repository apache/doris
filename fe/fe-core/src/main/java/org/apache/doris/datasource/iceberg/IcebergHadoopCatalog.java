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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.hadoop.IcebergHadoopFileIO;
import org.apache.doris.datasource.iceberg.hadoop.IcebergHadoopTableOperations;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.LockManagers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IcebergHadoopCatalog extends HadoopCatalog {
    private String warehouseLocation;
    protected Configuration conf;
    private FileSystem fs;
    private LockManager lockManager;
    protected FileIO fileIO;
    protected String uid;

    public void initialize(String name, Map<String, String> properties) {
        String inputWarehouseLocation = properties.get("warehouse");
        Preconditions.checkArgument(inputWarehouseLocation != null && inputWarehouseLocation.length() > 0,
                "Cannot initialize HadoopCatalog because warehousePath must not be null or empty");
        this.warehouseLocation = LocationUtil.stripTrailingSlash(inputWarehouseLocation);
        try {
            this.fs = new DFSFileSystem(properties).nativeFileSystem(warehouseLocation);
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
        this.fileIO = initializeFileIO(properties, conf);
        this.lockManager = LockManagers.from(properties);
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
                this.fileIO, this.conf, this.lockManager, this.fs);
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        Preconditions.checkArgument(namespace.levels().length >= 1,
                "Missing database in table identifier: %s", namespace);
        Path nsPath = new Path(this.warehouseLocation, Joiner.on("/").join(namespace.levels()));
        Set<TableIdentifier> tblIdents = Sets.newHashSet();

        try {
            if (!this.fs.getFileStatus(nsPath).isDirectory()) {
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
            return this.fs.listStatus(metadataPath, tableFilter).length >= 1;
        } catch (FileNotFoundException var4) {
            return false;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
