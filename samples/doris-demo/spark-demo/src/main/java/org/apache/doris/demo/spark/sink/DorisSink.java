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

package org.apache.doris.demo.spark.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.doris.demo.spark.util.DorisStreamLoad;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Map;


@Slf4j
public class DorisSink extends ForeachWriter<Row> implements Serializable {

    private Map<String, Object> parameters;
    private DorisStreamLoad dorisStreamLoad;


    public DorisSink(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    @Override
    public boolean open(long partitionId, long epochId) {
        String hostPort = parameters.get("hostPort").toString();
        String db = parameters.get("db").toString();
        String tbl = parameters.get("tbl").toString();
        String user = parameters.get("user").toString();
        String password = parameters.get("password").toString();
        dorisStreamLoad = new DorisStreamLoad(hostPort, db, tbl, user, password, "", "");
        return true;
    }

    @Override
    public void process(Row recode) {

        DorisStreamLoad.LoadResponse loadResponse = dorisStreamLoad.loadBatch(recode.getAs("value"));
        log.info(loadResponse.respContent);
    }

    @Override
    public void close(Throwable errorOrNull) {
    }
}
