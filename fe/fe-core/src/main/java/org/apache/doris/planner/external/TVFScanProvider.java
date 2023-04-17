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
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Lists;

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
        this.splitter = new TVFSplitter(tableValuedFunction);
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
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        return tableValuedFunction.getLocationProperties();
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return Lists.newArrayList();
    }

    @Override
    public TableIf getTargetTable() {
        return tvfTable;
    }
}
