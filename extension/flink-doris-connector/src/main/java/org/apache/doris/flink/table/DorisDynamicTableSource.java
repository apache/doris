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

package org.apache.doris.flink.table;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The {@link DorisDynamicTableSource} is used during planning.
 *
 * <p>In our example, we don't implement any of the available ability interfaces such as {@link SupportsFilterPushDown}
 * or {@link SupportsProjectionPushDown}. Therefore, the main logic can be found in {@link #getScanRuntimeProvider(ScanContext)}
 * where we instantiate the required {@link SourceFunction} and its {@link DeserializationSchema} for
 * runtime. Both instances are parameterized to return internal data structures (i.e. {@link RowData}).
 */
public final class DorisDynamicTableSource implements ScanTableSource, LookupTableSource {

    private final DorisOptions options;
    private final DorisReadOptions readOptions;
    private TableSchema physicalSchema;
    private static final Logger LOG = LoggerFactory.getLogger(DorisRowDataInputFormat.class);

    public DorisDynamicTableSource(DorisOptions options, DorisReadOptions readOptions, TableSchema physicalSchema) {
        this.options = options;
        this.readOptions = readOptions;
        this.physicalSchema = physicalSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // in our example the format decides about the changelog mode
        // but it could also be the source itself
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        List<PartitionDefinition> dorisPartitions;
        try {
            dorisPartitions = RestService.findPartitions(options, readOptions, LOG);
        } catch (DorisException e) {
            throw new RuntimeException("Failed fetch doris partitions");
        }
        DorisRowDataInputFormat.Builder builder = DorisRowDataInputFormat.builder()
                .setFenodes(options.getFenodes())
                .setUsername(options.getUsername())
                .setPassword(options.getPassword())
                .setTableIdentifier(options.getTableIdentifier())
                .setPartitions(dorisPartitions)
                .setReadOptions(readOptions);
        return InputFormatProvider.of(builder.build());
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        return null;
    }

    @Override
    public DynamicTableSource copy() {
        return new DorisDynamicTableSource(options, readOptions, physicalSchema);
    }

    @Override
    public String asSummaryString() {
        return "Doris Table Source";
    }
}
