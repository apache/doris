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
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.StringJoiner;

/**
 * DorisDynamicOutputFormat
 **/
public class DorisDynamicOutputFormat extends RichOutputFormat<RowData>  {


    private  DorisOptions options ;
    private DorisStreamLoad dorisStreamLoad;
    private final String fieldDelimiter = "\t";

    public DorisDynamicOutputFormat(DorisOptions options) {
        this.options = options;
    }

    @Override
    public void configure(Configuration configuration) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        dorisStreamLoad = new DorisStreamLoad(
                options.getFenodes(),
                options.getTableIdentifier().split("\\.")[0],
                options.getTableIdentifier().split("\\.")[1],
                options.getUsername(),
                options.getPassword());
        System.out.println(dorisStreamLoad);
    }

    @Override
    public void writeRecord(RowData row) throws IOException {
        StringJoiner value = new StringJoiner(this.fieldDelimiter);
        GenericRowData rowData = (GenericRowData) row;
        for(int i = 0; i < row.getArity(); ++i) {
            value.add(rowData.getField(i).toString());
        }
        DorisStreamLoad.LoadResponse loadResponse = dorisStreamLoad.loadBatch(value.toString());
        System.out.println(loadResponse);
    }

    @Override
    public void close() throws IOException {

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
     * Builder for {@link DorisDynamicOutputFormat}.
     */
    public static class Builder {
        private DorisOptions.Builder optionsBuilder;


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

        public DorisDynamicOutputFormat build() {
            return new DorisDynamicOutputFormat(
                    optionsBuilder.build()
            );
        }
    }
}
