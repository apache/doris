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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.CassandraExternalTable;
import org.apache.doris.catalog.external.MaxComputeExternalTable;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.MaxComputeExternalCatalog;
import org.apache.doris.datasource.cassandra.CassandraExternalCatalog;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.hadoop.fs.Path;

import java.util.*;

public class CassandraScanNode extends FileQueryScanNode {

    private final CassandraExternalTable table;
    public CassandraScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        this(id, desc, "CassandraScanNode", StatisticalType.CASSANDRA_SCAN_NODE, needCheckColumnPriv);
    }

    public CassandraScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                              StatisticalType statisticalType, boolean needCheckColumnPriv) {
        super(id, desc, planNodeName, statisticalType, needCheckColumnPriv);
        table = (CassandraExternalTable) desc.getTable();
    }

    @Override
    protected TFileType getLocationType() throws UserException {
        return getLocationType(null);
    }

    @Override
    protected TFileType getLocationType(String location) throws UserException {
        return TFileType.FILE_NET;
    }

    @Override
    protected TFileFormatType getFileFormatType() throws UserException {
        return TFileFormatType.FORMAT_JNI;
    }

    @Override
    protected List<String> getPathPartitionKeys() {
        return Collections.emptyList();
    }

    @Override
    protected TableIf getTargetTable() throws UserException {
        return table;
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        return new HashMap<>();
    }

    @Override
    protected List<Split> getSplits() throws UserException {
        List<Split> result = new ArrayList<>();
        result.add(new FileSplit(new Path("/"), 0, -1, -1, 0L, new String[0], Collections.emptyList()));
        return result;
    }


}
