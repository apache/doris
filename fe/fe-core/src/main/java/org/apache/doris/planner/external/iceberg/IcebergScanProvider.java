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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.external.IcebergSplitter;
import org.apache.doris.planner.external.QueryScanProvider;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionField;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Collectors;

/**
 * A file scan provider for iceberg.
 */
public class IcebergScanProvider extends QueryScanProvider {

    public static final int MIN_DELETE_FILE_SUPPORT_VERSION = 2;
    private final Analyzer analyzer;
    private final IcebergSource icebergSource;

    public IcebergScanProvider(IcebergSource icebergSource, Analyzer analyzer) {
        this.icebergSource = icebergSource;
        this.analyzer = analyzer;
        this.splitter = new IcebergSplitter(icebergSource, analyzer);
    }

    public static void setIcebergParams(TFileRangeDesc rangeDesc, IcebergSplit icebergSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(icebergSplit.getTableFormatType().value());
        TIcebergFileDesc fileDesc = new TIcebergFileDesc();
        int formatVersion = icebergSplit.getFormatVersion();
        fileDesc.setFormatVersion(formatVersion);
        if (formatVersion < MIN_DELETE_FILE_SUPPORT_VERSION) {
            fileDesc.setContent(FileContent.DATA.id());
        } else {
            for (IcebergDeleteFileFilter filter : icebergSplit.getDeleteFileFilters()) {
                TIcebergDeleteFileDesc deleteFileDesc = new TIcebergDeleteFileDesc();
                deleteFileDesc.setPath(filter.getDeleteFilePath());
                if (filter instanceof IcebergDeleteFileFilter.PositionDelete) {
                    fileDesc.setContent(FileContent.POSITION_DELETES.id());
                    IcebergDeleteFileFilter.PositionDelete positionDelete =
                            (IcebergDeleteFileFilter.PositionDelete) filter;
                    OptionalLong lowerBound = positionDelete.getPositionLowerBound();
                    OptionalLong upperBound = positionDelete.getPositionUpperBound();
                    if (lowerBound.isPresent()) {
                        deleteFileDesc.setPositionLowerBound(lowerBound.getAsLong());
                    }
                    if (upperBound.isPresent()) {
                        deleteFileDesc.setPositionUpperBound(upperBound.getAsLong());
                    }
                } else {
                    fileDesc.setContent(FileContent.EQUALITY_DELETES.id());
                    IcebergDeleteFileFilter.EqualityDelete equalityDelete =
                            (IcebergDeleteFileFilter.EqualityDelete) filter;
                    deleteFileDesc.setFieldIds(equalityDelete.getFieldIds());
                }
                fileDesc.addToDeleteFiles(deleteFileDesc);
            }
        }
        tableFormatFileDesc.setIcebergParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    public TFileType getLocationType() throws DdlException, MetaNotFoundException {
        String location = icebergSource.getIcebergTable().location();
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
        throw new DdlException("Unknown file location " + location
            + " for hms table " + icebergSource.getIcebergTable().name());
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return icebergSource.getIcebergTable().spec().fields().stream().map(PartitionField::name)
                .collect(Collectors.toList());
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        TFileFormatType type;
        String icebergFormat = icebergSource.getFileFormat();
        if (icebergFormat.equalsIgnoreCase("parquet")) {
            type = TFileFormatType.FORMAT_PARQUET;
        } else if (icebergFormat.equalsIgnoreCase("orc")) {
            type = TFileFormatType.FORMAT_ORC;
        } else {
            throw new DdlException(String.format("Unsupported format name: %s for iceberg table.", icebergFormat));
        }
        return type;
    }

    @Override
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        return icebergSource.getCatalog().getProperties();
    }

    @Override
    public TableIf getTargetTable() {
        return icebergSource.getTargetTable();
    }

    @Override
    public TFileAttributes getFileAttributes() throws UserException {
        return icebergSource.getFileAttributes();
    }
}
