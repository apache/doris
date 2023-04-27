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

package org.apache.doris.planner.external;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.ColumnRange;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Maps;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A HiveScanProvider to get information for scan node.
 */
public class HiveScanProvider extends HMSTableScanProvider {
    private static final Logger LOG = LogManager.getLogger(HiveScanProvider.class);

    private static final String PROP_FIELD_DELIMITER = "field.delim";
    private static final String DEFAULT_FIELD_DELIMITER = "\1"; // "\x01"
    private static final String DEFAULT_LINE_DELIMITER = "\n";

    protected HMSExternalTable hmsTable;
    protected final TupleDescriptor desc;
    protected Map<String, ColumnRange> columnNameToRange;

    public HiveScanProvider(HMSExternalTable hmsTable, TupleDescriptor desc,
            Map<String, ColumnRange> columnNameToRange) {
        this.hmsTable = hmsTable;
        this.desc = desc;
        this.columnNameToRange = columnNameToRange;
        this.splitter = new HiveSplitter(hmsTable, columnNameToRange);
    }

    @Override
    public TableIf getTargetTable() {
        return hmsTable;
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        TFileFormatType type = null;
        String inputFormatName = getRemoteHiveTable().getSd().getInputFormat();
        String hiveFormat = HiveMetaStoreClientHelper.HiveFileFormat.getFormat(inputFormatName);
        if (hiveFormat.equals(HiveMetaStoreClientHelper.HiveFileFormat.PARQUET.getDesc())) {
            type = TFileFormatType.FORMAT_PARQUET;
        } else if (hiveFormat.equals(HiveMetaStoreClientHelper.HiveFileFormat.ORC.getDesc())) {
            type = TFileFormatType.FORMAT_ORC;
        } else if (hiveFormat.equals(HiveMetaStoreClientHelper.HiveFileFormat.TEXT_FILE.getDesc())) {
            type = TFileFormatType.FORMAT_CSV_PLAIN;
        }
        return type;
    }

    @Override
    public TFileType getLocationType() throws DdlException, MetaNotFoundException {
        String location = hmsTable.getRemoteTable().getSd().getLocation();
        if (location != null && !location.isEmpty()) {
            if (location.startsWith(FeConstants.FS_PREFIX_S3)
                    || location.startsWith(FeConstants.FS_PREFIX_S3A)
                    || location.startsWith(FeConstants.FS_PREFIX_S3N)
                    || location.startsWith(FeConstants.FS_PREFIX_BOS)
                    || location.startsWith(FeConstants.FS_PREFIX_COS)
                    || location.startsWith(FeConstants.FS_PREFIX_OSS)
                    || location.startsWith(FeConstants.FS_PREFIX_OBS)) {
                return TFileType.FILE_S3;
            } else if (location.startsWith(FeConstants.FS_PREFIX_HDFS)) {
                return TFileType.FILE_HDFS;
            } else if (location.startsWith(FeConstants.FS_PREFIX_FILE)) {
                return TFileType.FILE_LOCAL;
            } else if (location.startsWith(FeConstants.FS_PREFIX_OFS)) {
                return TFileType.FILE_BROKER;
            } else if (location.startsWith(FeConstants.FS_PREFIX_GFS)) {
                return TFileType.FILE_BROKER;
            } else if (location.startsWith(FeConstants.FS_PREFIX_JFS)) {
                return TFileType.FILE_BROKER;
            }
        }
        throw new DdlException("Unknown file location " + location + " for hms table " + hmsTable.getName());
    }

    @Override
    public String getMetaStoreUrl() {
        return hmsTable.getMetastoreUri();
    }

    public int getTotalPartitionNum() {
        return ((HiveSplitter) splitter).getTotalPartitionNum();
    }

    public int getReadPartitionNum() {
        return ((HiveSplitter) splitter).getReadPartitionNum();
    }

    @Override
    public Table getRemoteHiveTable() throws DdlException, MetaNotFoundException {
        return hmsTable.getRemoteTable();
    }

    @Override
    public Map<String, String> getTableProperties() throws MetaNotFoundException {
        // TODO: implement it when we really properties from remote table.
        return Maps.newHashMap();
    }

    @Override
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        return hmsTable.getCatalogProperties();
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return getRemoteHiveTable().getPartitionKeys().stream().map(FieldSchema::getName).collect(Collectors.toList());
    }

    @Override
    public TFileAttributes getFileAttributes() throws UserException {
        TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
        textParams.setColumnSeparator(hmsTable.getRemoteTable().getSd().getSerdeInfo().getParameters()
                .getOrDefault(PROP_FIELD_DELIMITER, DEFAULT_FIELD_DELIMITER));
        textParams.setLineDelimiter(DEFAULT_LINE_DELIMITER);
        TFileAttributes fileAttributes = new TFileAttributes();
        fileAttributes.setTextParams(textParams);
        fileAttributes.setHeaderType("");
        return fileAttributes;
    }
}


