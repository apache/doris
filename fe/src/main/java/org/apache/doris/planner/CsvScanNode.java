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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.PartitionLoadInfo;
import org.apache.doris.load.Source;
import org.apache.doris.load.TableLoadInfo;
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TCsvScanNode;
import org.apache.doris.thrift.TMiniLoadEtlFunction;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class CsvScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(CsvScanNode.class);

    private final OlapTable table;
    private final LoadJob job;

    private List<String> filePaths = Lists.newArrayList();

    private String columnSeparator;
    private String lineDelimiter;

    private List<String> columns = Lists.newArrayList();
    private List<String> unspecifiedColumns = Lists.newArrayList();
    private List<String> defaultValues = Lists.newArrayList();

    private Map<String, TColumnType> columnTypeMapping = Maps.newHashMap();
    private Map<String, TMiniLoadEtlFunction> columnToFunction = Maps.newHashMap();

    private double maxFilterRatio = 0.0;

    public CsvScanNode(PlanNodeId id, TupleDescriptor desc, OlapTable table, LoadJob job) {
        super(id, desc, "Scan CSV");
        this.table = table;
        this.job = job;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.CSV_SCAN_NODE;
        msg.csv_scan_node = new TCsvScanNode(desc.getId().asInt(), filePaths);

        if (!Strings.isNullOrEmpty(columnSeparator)) {
            msg.csv_scan_node.setColumn_separator(columnSeparator);
        }
        if (!Strings.isNullOrEmpty(lineDelimiter)) {
            msg.csv_scan_node.setLine_delimiter(lineDelimiter);
        }

        if (!columns.isEmpty()) {
            msg.csv_scan_node.setColumns(columns);
        }
        if (!unspecifiedColumns.isEmpty()) {
            msg.csv_scan_node.setUnspecified_columns(unspecifiedColumns);
        }
        if (!defaultValues.isEmpty()) {
            msg.csv_scan_node.setDefault_values(defaultValues);
        }

        if (!columnToFunction.isEmpty()) {
            msg.csv_scan_node.setColumn_function_mapping(columnToFunction);
        }
        msg.csv_scan_node.setColumn_type_mapping(columnTypeMapping);
        msg.csv_scan_node.setMax_filter_ratio(maxFilterRatio);
        msg.csv_scan_node.setColumn_separator(columnSeparator);
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        // get file paths
        // file paths in different partitions are same in mini load
        TableLoadInfo tableLoadInfo = job.getTableLoadInfo(table.getId());
        Collection<PartitionLoadInfo> partitionLoadInfos = tableLoadInfo.getIdToPartitionLoadInfo().values();
        Preconditions.checkState(!partitionLoadInfos.isEmpty());
        PartitionLoadInfo partitionLoadInfo = (PartitionLoadInfo) partitionLoadInfos.toArray()[0];
        List<Source> sources = partitionLoadInfo.getSources();
        Preconditions.checkState(sources.size() == 1);
        Source source = sources.get(0);
        filePaths = source.getFileUrls();

        // column separator
        columnSeparator = source.getColumnSeparator();

        // line delimiter
        lineDelimiter = source.getLineDelimiter();

        // construct columns (specified & unspecified) and default-values
        List<String> columnNames = Lists.newArrayList();
        for (Column column : table.getBaseSchema()) {
            columnNames.add(column.getName());
        }
        columns = source.getColumnNames();
        if (columns.isEmpty()) {
            columns = columnNames;
        }
        for (String columnName : columns) {
            if (!columnNames.contains(columnName)) {
                LOG.info("Column [{}] is not exist in table schema, will be ignored.", columnName);
            }
        }
        for (String columnName : columnNames) {
            Column column = table.getColumn(columnName);
            columnTypeMapping.put(columnName, column.getOriginType().toColumnTypeThrift());

            if (columns.contains(columnName)) {
                continue;
            }
            unspecifiedColumns.add(columnName);
            String defaultValue = column.getDefaultValue();
            if (defaultValue == null && false == column.isAllowNull()) {
                throw new UserException(
                        "Column [" + columnName + "] should be specified. "
                                + "only columns have default values can be omitted");
            }
            if (true == column.isAllowNull() && null == defaultValue) {
                defaultValues.add("\\N");
            } else {
                defaultValues.add(defaultValue);
            }
        }

        Map<String, Pair<String, List<String>>> functions = source.getColumnToFunction();
        for (String key : functions.keySet()) {
            final Pair<String, List<String>> pair = functions.get(key);
            TMiniLoadEtlFunction function = new TMiniLoadEtlFunction();
            int paramColumnIndex = -1; 
            for (String str : pair.second) {
                boolean find = false;
                for (int i = 0; i < columns.size(); i++) {
                    if (str.equals(columns.get(i))) {
                        paramColumnIndex = i;
                        find = true;
                        break;
                    }   
                }   
                if (find) {
                    function.setFunction_name(pair.first);
                    function.setParam_column_index(paramColumnIndex);
                    columnToFunction.put(key, function); 
                    break;
                }   
            }   
        } 
        // max filter ratio
        // TODO: remove!!
        maxFilterRatio = job.getMaxFilterRatio();
    }

    @Override
    protected String debugString() {
        ToStringHelper helper = MoreObjects.toStringHelper(this);
        return helper.addValue(super.debugString()).toString();
    }

    /**
     * like Mysql, We query Meta to get request's data localtion
     * extra result info will pass to backend ScanNode
     */
    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return null;
    }

    @Override
    public int getNumInstances() {
        return 1;
    }
}

