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

package org.apache.doris.planner.external.deltalake;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.external.DeltaLakeExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.external.HiveScanNode;
import org.apache.doris.planner.external.TableFormatType;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TDeltaLakeFileDesc;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import avro.shaded.com.google.common.base.Preconditions;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.DeltaScan;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeltaLakeScanNode extends HiveScanNode {
    private static DeltaLakeSource source = null;
    private static final Logger LOG = LoggerFactory.getLogger(DeltaLakeScanNode.class);
    private static final Base64.Encoder BASE64_ENCODER =
            java.util.Base64.getUrlEncoder().withoutPadding();

    public DeltaLakeScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "DELTALAKE_SCAN_NODE", StatisticalType.DELTALAKE_SCAN_NODE, needCheckColumnPriv);
    }

    @Override
    protected void doInitialize() throws UserException {
        DeltaLakeExternalTable table = (DeltaLakeExternalTable) desc.getTable();
        if (table.isView()) {
            throw new AnalysisException(
                String.format("Querying external view '%s.%s' is not supported", table.getDbName(),
                    table.getName()));
        }
        computeColumnFilter();
        initBackendPolicy();
        source = new DeltaLakeSource(table, desc, columnNameToRange);
        Preconditions.checkNotNull(source);
        initSchemaParams();
    }

    public static void setDeltaLakeParams(TFileRangeDesc rangeDesc, DeltaLakeSplit deltaLakeSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(deltaLakeSplit.getTableFormatType().value());
        TDeltaLakeFileDesc fileDesc = new TDeltaLakeFileDesc();
        fileDesc.setDbName(source.getDeltalakeExtTable().getDbName());
        fileDesc.setTableName(source.getDeltalakeExtTable().getName());
        LOG.debug("params:{}", deltaLakeSplit.getPath().toString());
        fileDesc.setPath(deltaLakeSplit.getPath().toString());
        fileDesc.setConf(encodeConfToString(
                HiveMetaStoreClientHelper.getConfiguration(source.getDeltalakeExtTable())));
        fileDesc.setDeltaLakeColumnNames(source.getDesc().getSlots().stream().map(slot -> slot.getColumn().getName())
                .collect(Collectors.joining(",")));
        tableFormatFileDesc.setDeltaLakeParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    public List<Split> getSplits() throws UserException {
        List<Split> splits = new ArrayList<>();
        DeltaLakeExternalTable table = source.getDeltalakeExtTable();
        String path = table.getRemoteTable().getSd().getLocation();
        Configuration conf = HiveMetaStoreClientHelper.getConfiguration(table);
        Snapshot snapshot = DeltaLog.forTable(conf, path).snapshot();
        List<String> partitionColumns = snapshot.getMetadata().getPartitionColumns();
        DeltaScan deltaScan = snapshot.scan();
        CloseableIterator<AddFile> allFiles = deltaScan.getFiles();
        while (allFiles.hasNext()) {
            AddFile addFile = allFiles.next();
            DeltaLakeSplit deltaLakeSplit = getDeltaLakeSplit(addFile, partitionColumns, conf);
            splits.add(deltaLakeSplit);
        }
        try {
            allFiles.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return splits;
    }

    @NotNull
    private static DeltaLakeSplit getDeltaLakeSplit(AddFile addFile, List<String> partitionColumns,
                                                    Configuration configuration) {
        List<String> partitionValues = new ArrayList<>();
        Map<String, String> map = addFile.getPartitionValues();
        for (String key : partitionColumns) {
            if (map.containsKey(key)) {
                partitionValues.add(map.get(key));
            }
        }
        DeltaLakeSplit deltaLakeSplit = new DeltaLakeSplit(new Path(addFile.getPath()),
                    0, addFile.getSize(), addFile.getSize(), null, partitionValues, configuration);
        deltaLakeSplit.setTableFormatType(TableFormatType.DELTALAKE);
        return deltaLakeSplit;
    }

    @Override
    public TFileFormatType getFileFormatType() {
        return TFileFormatType.FORMAT_JNI;
    }

    public static String encodeConfToString(Configuration conf) {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream;
            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            conf.write(objectOutputStream);
            objectOutputStream.flush();
            objectOutputStream.close();
            byte[] bytes = byteArrayOutputStream.toByteArray();
            return new String(BASE64_ENCODER.encode(bytes), java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
