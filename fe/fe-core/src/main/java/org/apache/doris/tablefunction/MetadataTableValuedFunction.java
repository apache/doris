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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.tvf.source.MetadataScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataTableRequestParams;
import org.apache.doris.thrift.TMetadataType;

public abstract class MetadataTableValuedFunction extends TableValuedFunctionIf {

    public static Integer getColumnIndexFromColumnName(TMetadataType type, String columnName,
            TMetadataTableRequestParams params)
            throws AnalysisException {
        switch (type) {
            case BACKENDS:
                return BackendsTableValuedFunction.getColumnIndexFromColumnName(columnName);
            case FRONTENDS:
                return FrontendsTableValuedFunction.getColumnIndexFromColumnName(columnName);
            case FRONTENDS_DISKS:
                return FrontendsDisksTableValuedFunction.getColumnIndexFromColumnName(columnName);
            case ICEBERG:
                return IcebergTableValuedFunction.getColumnIndexFromColumnName(columnName);
            case CATALOGS:
                return CatalogsTableValuedFunction.getColumnIndexFromColumnName(columnName);
            case MATERIALIZED_VIEWS:
                return MvInfosTableValuedFunction.getColumnIndexFromColumnName(columnName);
            case PARTITIONS:
                return PartitionsTableValuedFunction.getColumnIndexFromColumnName(columnName, params);
            case JOBS:
                return JobsTableValuedFunction.getColumnIndexFromColumnName(columnName, params);
            case TASKS:
                return TasksTableValuedFunction.getColumnIndexFromColumnName(columnName, params);
            default:
                throw new AnalysisException("Unknown Metadata TableValuedFunction type");
        }
    }

    public abstract TMetadataType getMetadataType();

    public abstract TMetaScanRange getMetaScanRange();

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc) {
        return new MetadataScanNode(id, desc, this);
    }
}
