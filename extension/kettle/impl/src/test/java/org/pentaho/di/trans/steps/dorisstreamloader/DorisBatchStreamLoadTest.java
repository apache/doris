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

package org.pentaho.di.trans.steps.dorisstreamloader;

import org.junit.Ignore;
import org.junit.Test;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisBatchStreamLoad;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisOptions;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class DorisBatchStreamLoadTest {

    @Test
    @Ignore
    public void testStreamLoad() throws Exception {
        DorisOptions options = DorisOptions.builder()
                .withFenodes("127.0.0.1:8030")
                .withDatabase("test")
                .withTable("test_flink_c")
                .withUsername("root")
                .withPassword("")
                .withBufferFlushMaxBytes(10240000000L)
                .withBufferFlushMaxRows(100000)
                .withStreamLoadProp(new Properties())
                .build();
        DorisBatchStreamLoad streamLoad = new DorisBatchStreamLoad(options, new LogChannel());
        streamLoad.writeRecord(options.getDatabase(), options.getTable(), "zhangsan\t10".getBytes(StandardCharsets.UTF_8));

        while (!streamLoad.isLoadThreadAlive()){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }

        streamLoad.forceFlush();

        // stay main thread alive
        Thread.sleep(10000);
    }
}
