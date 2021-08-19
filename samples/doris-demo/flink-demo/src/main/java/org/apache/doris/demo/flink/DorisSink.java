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
package org.apache.doris.demo.flink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Custom doris sink
 */
public class DorisSink extends RichSinkFunction<String> {


    private static final Logger log = LoggerFactory.getLogger(DorisSink.class);

    private final static List<String> DORIS_SUCCESS_STATUS = new ArrayList<>(Arrays.asList("Success", "Publish Timeout"));

    private DorisStreamLoad dorisStreamLoad;

    private String columns;

    private String jsonFormat;

    public DorisSink(DorisStreamLoad dorisStreamLoad, String columns, String jsonFormat) {
        this.dorisStreamLoad = dorisStreamLoad;
        this.columns = columns;
        this.jsonFormat = jsonFormat;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }


    /**
     * Determine whether StreamLoad is successful
     *
     * @param respContent streamLoad returns the entity
     * @return
     */
    public static Boolean checkStreamLoadStatus(RespContent respContent) {
        if (DORIS_SUCCESS_STATUS.contains(respContent.getStatus())
                && respContent.getNumberTotalRows() == respContent.getNumberLoadedRows()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        DorisStreamLoad.LoadResponse loadResponse = dorisStreamLoad.loadBatch(value, columns, jsonFormat);
        if (loadResponse != null && loadResponse.status == 200) {
            RespContent respContent = JSON.parseObject(loadResponse.respContent, RespContent.class);
            if (!checkStreamLoadStatus(respContent)) {
                log.error("Stream Load fail{}:", loadResponse);
            }
        } else {
            log.error("Stream Load Request failed:{}", loadResponse);
        }
    }
}

