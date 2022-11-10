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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.planner.external.ExternalFileScanNode.ParamCreateContext;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TVFScanProvider extends QueryScanProvider {
    private FunctionGenTable tvfTable;
    private final TupleDescriptor desc;
    private ExternalFileTableValuedFunction tableValuedFunction;

    public TVFScanProvider(FunctionGenTable tvfTable, TupleDescriptor desc,
                            ExternalFileTableValuedFunction tableValuedFunction) {
        this.tvfTable = tvfTable;
        this.desc = desc;
        this.tableValuedFunction = tableValuedFunction;
    }

    public String getFsName() {
        return tableValuedFunction.getFsName();
    }

    // =========== implement abstract methods of QueryScanProvider =================
    @Override
    public TFileAttributes getFileAttributes() throws UserException {
        return tableValuedFunction.getFileAttributes();
    }


    // =========== implement interface methods of FileScanProviderIf ================
    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        return tableValuedFunction.getTFileFormatType();
    }

    @Override
    public TFileType getLocationType() throws DdlException, MetaNotFoundException {
        return tableValuedFunction.getTFileType();
    }

    @Override
    public List<InputSplit> getSplits(List<Expr> exprs) throws IOException, UserException {
        List<InputSplit> splits = Lists.newArrayList();
        List<TBrokerFileStatus> fileStatuses = tableValuedFunction.getFileStatuses();
        for (TBrokerFileStatus fileStatus : fileStatuses) {
            Path path = new Path(fileStatus.getPath());
            FileSplit fileSplit = new FileSplit(path, 0, fileStatus.getSize(), new String[0]);
            splits.add(fileSplit);
        }
        return splits;
    }

    @Override
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        return tableValuedFunction.getLocationProperties();
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return Lists.newArrayList();
    }

    @Override
    public ParamCreateContext createContext(Analyzer analyzer) throws UserException {
        ParamCreateContext context = new ParamCreateContext();
        context.params = new TFileScanRangeParams();
        context.destTupleDescriptor = desc;
        context.params.setDestTupleId(desc.getId().asInt());
        // no use, only for avoid null exception.
        context.fileGroup = new BrokerFileGroup(tvfTable.getId(), "", "");


        // Hive table must extract partition value from path and hudi/iceberg table keep
        // partition field in file.
        List<String> partitionKeys = getPathPartitionKeys();
        List<Column> columns = tvfTable.getBaseSchema(false);
        context.params.setNumOfColumnsFromFile(columns.size() - partitionKeys.size());
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }

            TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
            slotInfo.setSlotId(slot.getId().asInt());
            slotInfo.setIsFileSlot(!partitionKeys.contains(slot.getColumn().getName()));
            context.params.addToRequiredSlots(slotInfo);
        }
        return context;
    }

    @Override
    public TableIf getTargetTable() {
        return tvfTable;
    }
}
