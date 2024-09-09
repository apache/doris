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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.DataGenScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TDataGenFunctionName;

import java.util.List;

public abstract class DataGenTableValuedFunction extends TableValuedFunctionIf {
    public abstract List<TableValuedFunctionTask> getTasks() throws UserException;

    public abstract TDataGenFunctionName getDataGenFunctionName();

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc) {
        return new DataGenScanNode(id, desc, this);
    }
}
