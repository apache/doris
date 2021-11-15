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
package org.apache.doris.flink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.table.DorisDynamicOutputFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import java.io.IOException;

/**
 * example using {@link DorisDynamicOutputFormat} for batching.
 */
public class DorisOutPutFormatExample {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MapOperator<String, RowData> data = env.fromElements("")
                .map(new MapFunction<String, RowData>() {
                    @Override
                    public RowData map(String value) throws Exception {
                        GenericRowData genericRowData = new GenericRowData(3);
                        genericRowData.setField(0, StringData.fromString("北京"));
                        genericRowData.setField(1, 116.405419);
                        genericRowData.setField(2, 39.916927);
                        return genericRowData;
                    }
                });

        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("FE_IP:8030")
                .setTableIdentifier("db.table")
                .setUsername("root")
                .setPassword("").build();
        DorisReadOptions readOptions = DorisReadOptions.defaults();
        DorisExecutionOptions executionOptions = DorisExecutionOptions.defaults();

        LogicalType[] types = {new VarCharType(), new DoubleType(), new DoubleType()};
        String[] fiels = {"city", "longitude", "latitude"};

        DorisDynamicOutputFormat outputFormat =
                new DorisDynamicOutputFormat(dorisOptions, readOptions, executionOptions, types, fiels);

        try {
            outputFormat.open(0, 1);
            data.output(outputFormat);
            outputFormat.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        env.execute("doris batch sink example");


    }

}
