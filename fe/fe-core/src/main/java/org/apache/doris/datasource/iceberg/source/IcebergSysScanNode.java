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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.util.SerializationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class IcebergSysScanNode extends IcebergScanNode {
    public IcebergSysScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv, SessionVariable sv,
            ScanContext scanContext) {
        super(id, desc, needCheckColumnPriv, sv, scanContext);
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, org.apache.doris.spi.Split split) {
        if (split instanceof IcebergSplit) {
            setIcebergSysParams(rangeDesc, (IcebergSplit) split);
        }
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        return executeWithPreExecutionAuthenticator(new Callable<List<Split>>() {
            @Override
            public List<Split> call() throws Exception {
                return doGetSystemTableSplits();
            }
        });
    }

    @Override
    public boolean isBatchMode() {
        return false;
    }

    @Override
    public TFileFormatType getFileFormatType() {
        return TFileFormatType.FORMAT_JNI;
    }

    protected void setIcebergSysParams(TFileRangeDesc rangeDesc, IcebergSplit icebergSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(icebergSplit.getTableFormatType().value());
        tableFormatFileDesc.setTableLevelRowCount(-1);
        TIcebergFileDesc fileDesc = new TIcebergFileDesc();
        fileDesc.setSerializedSplit(icebergSplit.getSerializedSplit());
        tableFormatFileDesc.setIcebergParams(fileDesc);
        rangeDesc.setFormatType(TFileFormatType.FORMAT_JNI);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    protected int resolveFormatVersion(Table table) {
        if (table instanceof BaseTable) {
            return ((BaseTable) table).operations().current().formatVersion();
        }
        return MIN_DELETE_FILE_SUPPORT_VERSION;
    }

    protected List<Split> doGetSystemTableSplits() throws UserException {
        List<Split> splits = new ArrayList<>();
        TableScan scan = createTableScan();
        try (org.apache.iceberg.io.CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles()) {
            for (FileScanTask task : fileScanTasks) {
                splits.add(createIcebergSysSplit(task));
            }
        } catch (IOException e) {
            throw new UserException(e.getMessage(), e);
        }
        selectedPartitionNum = 0;
        return splits;
    }

    protected Split createIcebergSysSplit(FileScanTask fileScanTask) {
        long rowCount = fileScanTask.file() == null ? 1 : fileScanTask.file().recordCount();
        IcebergSplit split = IcebergSplit.newSysTableSplit(
                SerializationUtil.serializeToBase64(fileScanTask), rowCount);
        split.setTableFormatType(TableFormatType.ICEBERG);
        return split;
    }
}
