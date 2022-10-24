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

package org.apache.doris.analysis;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.NotImplementedException;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PartitionDesc {
    protected List<String> partitionColNames;
    protected List<SinglePartitionDesc> singlePartitionDescs;

    protected PartitionType type;

    public PartitionDesc(List<String> partitionColNames,
                         List<SinglePartitionDesc> singlePartitionDescs) {
        this.partitionColNames = partitionColNames;
        this.singlePartitionDescs = singlePartitionDescs;
        if (this.singlePartitionDescs == null) {
            this.singlePartitionDescs = Lists.newArrayList();
        }
    }

    public List<SinglePartitionDesc> getSinglePartitionDescs() {
        return this.singlePartitionDescs;
    }

    public List<String> getPartitionColNames() {
        return partitionColNames;
    }

    public void analyze(List<ColumnDef> columnDefs, Map<String, String> otherProperties) throws AnalysisException {
        if (partitionColNames == null || partitionColNames.isEmpty()) {
            throw new AnalysisException("No partition columns.");
        }

        // `analyzeUniqueKeyMergeOnWrite` would modify `properties`, which will be used later,
        // so we just clone a properties map here.
        boolean enableUniqueKeyMergeOnWrite = false;
        if (otherProperties != null) {
            enableUniqueKeyMergeOnWrite =
                PropertyAnalyzer.analyzeUniqueKeyMergeOnWrite(Maps.newHashMap(otherProperties));
        }
        Set<String> partColNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (String partitionCol : partitionColNames) {
            if (!partColNames.add(partitionCol)) {
                throw new AnalysisException("Duplicated partition column " + partitionCol);
            }

            boolean found = false;
            for (ColumnDef columnDef : columnDefs) {
                if (columnDef.getName().equals(partitionCol)) {
                    if (!columnDef.isKey() && (columnDef.getAggregateType() != AggregateType.NONE
                            || enableUniqueKeyMergeOnWrite)) {
                        throw new AnalysisException("The partition column could not be aggregated column");
                    }
                    if (columnDef.getType().isFloatingPointType()) {
                        throw new AnalysisException("Floating point type column can not be partition column");
                    }
                    if (columnDef.getType().isScalarType(PrimitiveType.STRING)) {
                        throw new AnalysisException("String Type should not be used in partition column["
                                + columnDef.getName() + "].");
                    }
                    if (!ConnectContext.get().getSessionVariable().isAllowPartitionColumnNullable()
                            && columnDef.isAllowNull()) {
                        throw new AnalysisException("The partition column must be NOT NULL");
                    }
                    if (this instanceof ListPartitionDesc && columnDef.isAllowNull()) {
                        throw new AnalysisException("The list partition column must be NOT NULL");
                    }
                    found = true;
                    break;
                }
            }

            if (!found) {
                throw new AnalysisException("Partition column[" + partitionCol + "] does not exist in column list.");
            }
        }

        Set<String> nameSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (SinglePartitionDesc desc : singlePartitionDescs) {
            if (!nameSet.add(desc.getPartitionName())) {
                throw new AnalysisException("Duplicated partition name: " + desc.getPartitionName());
            }
            // in create table stmt, we use given properties
            // copy one. because ProperAnalyzer will remove entry after analyze
            Map<String, String> givenProperties = null;
            if (otherProperties != null) {
                givenProperties = Maps.newHashMap(otherProperties);
            }
            // check partitionType
            checkPartitionKeyValueType(desc.getPartitionKeyDesc());
            // analyze singlePartitionDesc
            desc.analyze(columnDefs.size(), givenProperties);
        }
    }

    public void checkPartitionKeyValueType(PartitionKeyDesc partitionKeyDesc) throws AnalysisException {

    }

    public PartitionType getType() {
        return type;
    }

    public String toSql() {
        throw new NotImplementedException();
    }

    public PartitionInfo toPartitionInfo(List<Column> schema, Map<String, Long> partitionNameToId, boolean isTemp)
            throws DdlException {
        throw new NotImplementedException();
    }
}
