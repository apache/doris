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
import org.apache.doris.flink.datastream.ScalaValueReader;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * InputFormat for {@link DorisDynamicTableSource}.
 */
@Internal
public class DorisRowDataInputFormat extends RichInputFormat<RowData, DorisTableInputSplit> implements ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DorisRowDataInputFormat.class);

    private DorisOptions options;
    private DorisReadOptions readOptions;
    private List<PartitionDefinition> dorisPartitions;
    private TypeInformation<RowData> rowDataTypeInfo;

    private ScalaValueReader scalaValueReader;
    private transient boolean hasNext;

    public DorisRowDataInputFormat(DorisOptions options, List<PartitionDefinition> dorisPartitions, DorisReadOptions readOptions) {
        this.options = options;
        this.dorisPartitions = dorisPartitions;
        this.readOptions = readOptions;
    }

    @Override
    public void configure(Configuration parameters) {
        //do nothing here
    }

    @Override
    public void openInputFormat() {
        //called once per inputFormat (on open)
    }

    @Override
    public void closeInputFormat() {
        //called once per inputFormat (on close)
    }

    /**
     * Connects to the source database and executes the query in a <b>parallel
     * fashion</b> if
     * this {@link InputFormat} is built using a parameterized query (i.e. using
     * a {@link PreparedStatement})
     * and a proper {@link  }, in a <b>non-parallel
     * fashion</b> otherwise.
     *
     * @param inputSplit which is ignored if this InputFormat is executed as a
     *                   non-parallel source,
     *                   a "hook" to the query parameters otherwise (using its
     *                   <i>splitNumber</i>)
     * @throws IOException if there's an error during the execution of the query
     */
    @Override
    public void open(DorisTableInputSplit inputSplit) throws IOException {
        scalaValueReader = new ScalaValueReader(inputSplit.partition, options, readOptions);
        hasNext = scalaValueReader.hasNext();
    }

    /**
     * Closes all resources used.
     *
     * @throws IOException Indicates that a resource could not be closed.
     */
    @Override
    public void close() throws IOException {

    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return rowDataTypeInfo;
    }

    /**
     * Checks whether all data has been read.
     *
     * @return boolean value indication whether all data has been read.
     * @throws IOException
     */
    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    /**
     * Stores the next resultSet row in a tuple.
     *
     * @param reuse row to be reused.
     * @return row containing next {@link RowData}
     * @throws IOException
     */
    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        if (!hasNext) {
            return null;
        }
        List next = (List) scalaValueReader.next();
        GenericRowData genericRowData = new GenericRowData(next.size());
        for (int i = 0; i < next.size(); i++) {
            genericRowData.setField(i, next.get(i));
        }
        //update hasNext after we've read the record
        hasNext = scalaValueReader.hasNext();
        return genericRowData;
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public DorisTableInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        List<DorisTableInputSplit> dorisSplits = new ArrayList<>();
        int splitNum = 0;
        for (PartitionDefinition partition : dorisPartitions) {
            dorisSplits.add(new DorisTableInputSplit(splitNum++, partition));
        }
        LOG.info("DorisTableInputSplit Num:{}", dorisSplits.size());
        return dorisSplits.toArray(new DorisTableInputSplit[0]);
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(DorisTableInputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    /**
     * A builder used to set parameters to the output format's configuration in a fluent way.
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link DorisRowDataInputFormat}.
     */
    public static class Builder {
        private DorisOptions.Builder optionsBuilder;
        private List<PartitionDefinition> partitions;
        private DorisReadOptions readOptions;


        public Builder() {
            this.optionsBuilder = DorisOptions.builder();
        }

        public Builder setFenodes(String fenodes) {
            this.optionsBuilder.setFenodes(fenodes);
            return this;
        }

        public Builder setUsername(String username) {
            this.optionsBuilder.setUsername(username);
            return this;
        }

        public Builder setPassword(String password) {
            this.optionsBuilder.setPassword(password);
            return this;
        }

        public Builder setTableIdentifier(String tableIdentifier) {
            this.optionsBuilder.setTableIdentifier(tableIdentifier);
            return this;
        }

        public Builder setPartitions(List<PartitionDefinition> partitions) {
            this.partitions = partitions;
            return this;
        }

        public Builder setReadOptions(DorisReadOptions readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        public DorisRowDataInputFormat build() {
            return new DorisRowDataInputFormat(
                    optionsBuilder.build(), partitions, readOptions
            );
        }
    }
}
