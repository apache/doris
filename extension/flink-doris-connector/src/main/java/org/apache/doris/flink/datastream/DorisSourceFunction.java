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
import org.apache.doris.flink.cfg.FlinkSettings;
import org.apache.doris.flink.cfg.Settings;
import org.apache.doris.flink.deserialization.DorisDeserializationSchema;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


/**
 * DorisSource
 **/

public class DorisSourceFunction<T> extends RichSourceFunction<T> implements ResultTypeQueryable<T> {

    private  static final Logger logger = LoggerFactory.getLogger(DorisSourceFunction.class);

    private DorisDeserializationSchema deserializer;
    private Settings settings;
    private List<PartitionDefinition>  dorisPartitions;
    private ScalaValueReader scalaValueReader;


    public DorisSourceFunction(DorisOptions options, DorisDeserializationSchema deserializer) throws DorisException {
        this(new Configuration(),options,deserializer);
    }

    public DorisSourceFunction(Configuration flinkConf, DorisOptions options, DorisDeserializationSchema deserializer) throws DorisException {
        this.settings = initSetting(flinkConf,options);
        this.deserializer = deserializer;
        this.dorisPartitions =  RestService.findPartitions(settings,logger);
    }

    private FlinkSettings initSetting(Configuration flinkConf,DorisOptions options){
        FlinkSettings flinkSettings = new FlinkSettings(flinkConf);
        flinkSettings.init(options);
        return flinkSettings;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(SourceContext sourceContext) throws Exception{
        for(PartitionDefinition partitions : dorisPartitions){
            scalaValueReader = new ScalaValueReader(partitions, settings);
            while (scalaValueReader.hasNext()){
                Object next = scalaValueReader.next();
                sourceContext.collect(next);
            }
        }
    }

    @Override
    public void cancel() {
        scalaValueReader.close();
    }


    @Override
    public TypeInformation<T> getProducedType() {
        return this.deserializer.getProducedType();
    }
}
