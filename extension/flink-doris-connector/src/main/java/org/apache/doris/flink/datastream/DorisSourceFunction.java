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
package org.apache.doris.flink.datastream;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisStreamOptions;
import org.apache.doris.flink.deserialization.DorisDeserializationSchema;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * DorisSource
 **/

public class DorisSourceFunction extends RichParallelSourceFunction<List<?>> implements ResultTypeQueryable<List<?>> {

    private static final Logger logger = LoggerFactory.getLogger(DorisSourceFunction.class);

    private final DorisDeserializationSchema<List<?>> deserializer;
    private final DorisOptions options;
    private final DorisReadOptions readOptions;
    private transient volatile boolean isRunning;
    private List<PartitionDefinition> dorisPartitions;
    private List<PartitionDefinition> taskDorisPartitions = Lists.newArrayList();

    public DorisSourceFunction(DorisStreamOptions streamOptions, DorisDeserializationSchema<List<?>> deserializer) {
        this.deserializer = deserializer;
        this.options = streamOptions.getOptions();
        this.readOptions = streamOptions.getReadOptions();
        try {
            this.dorisPartitions = RestService.findPartitions(options, readOptions, logger);
            logger.info("Doris partitions size {}", dorisPartitions.size());
        } catch (DorisException e) {
            throw new RuntimeException("Failed fetch doris partitions");
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.isRunning = true;
        assignTaskPartitions();
    }

    /**
     * Assign patitions to each task.
     */
    private void assignTaskPartitions() {
        int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int totalTasks = getRuntimeContext().getNumberOfParallelSubtasks();

        for (int i = 0; i < dorisPartitions.size(); i++) {
            if (i % totalTasks == taskIndex) {
                taskDorisPartitions.add(dorisPartitions.get(i));
            }
        }
        logger.info("subtask {} process {} partitions ", taskIndex, taskDorisPartitions.size());
    }

    @Override
    public void run(SourceContext<List<?>> sourceContext) {
        for (PartitionDefinition partitions : taskDorisPartitions) {
            try (ScalaValueReader scalaValueReader = new ScalaValueReader(partitions, options, readOptions)) {
                while (isRunning && scalaValueReader.hasNext()) {
                    List<?> next = scalaValueReader.next();
                    sourceContext.collect(next);
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        super.close();
        isRunning = false;
    }

    @Override
    public TypeInformation<List<?>> getProducedType() {
        return this.deserializer.getProducedType();
    }
}
