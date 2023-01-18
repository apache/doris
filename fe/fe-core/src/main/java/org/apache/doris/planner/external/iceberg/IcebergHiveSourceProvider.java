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

package org.apache.doris.planner.external.iceberg;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.HMSResource;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.planner.ColumnRange;
import org.apache.doris.planner.external.ExternalFileScanNode;
import org.apache.doris.planner.external.HiveScanProvider;
import org.apache.doris.thrift.TFileAttributes;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.HashMap;
import java.util.Map;

public class IcebergHiveSourceProvider implements IcebergSourceProvider {

    private HMSExternalTable hmsTable;
    private HiveScanProvider hiveScanProvider;

    public IcebergHiveSourceProvider(HMSExternalTable hmsTable, TupleDescriptor desc,
                                     Map<String, ColumnRange> columnNameToRange) {
        this.hiveScanProvider = new HiveScanProvider(hmsTable, desc, columnNameToRange);
        this.hmsTable = hmsTable;
    }

    @Override
    public String getFileFormat() throws DdlException, MetaNotFoundException {
        return hiveScanProvider.getRemoteHiveTable().getParameters()
                .getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    }

    public org.apache.iceberg.Table getIcebergTable() throws MetaNotFoundException {
        org.apache.iceberg.hive.HiveCatalog hiveCatalog = new org.apache.iceberg.hive.HiveCatalog();
        Configuration conf = hiveScanProvider.getConfiguration();
        hiveCatalog.setConf(conf);
        // initialize hive catalog
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(HMSResource.HIVE_METASTORE_URIS, hiveScanProvider.getMetaStoreUrl());
        catalogProperties.put("uri", hiveScanProvider.getMetaStoreUrl());
        hiveCatalog.initialize("hive", catalogProperties);
        return hiveCatalog.loadTable(TableIdentifier.of(hmsTable.getDbName(), hmsTable.getName()));
    }

    @Override
    public ExternalFileScanNode.ParamCreateContext createContext() throws UserException {
        return hiveScanProvider.createContext(null);
    }

    @Override
    public TableIf getTargetTable() {
        return hiveScanProvider.getTargetTable();
    }

    @Override
    public TFileAttributes getFileAttributes() throws UserException {
        return hiveScanProvider.getFileAttributes();
    }

    @Override
    public ExternalCatalog getCatalog() {
        return hmsTable.getCatalog();
    }
}
