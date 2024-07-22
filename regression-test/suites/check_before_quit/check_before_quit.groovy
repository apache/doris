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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("check_before_quit", "nonConcurrent,p0") {
    List<List<Object>> be_result = sql "show backends"
    List<Object> beRow = be_result.get(0)
    String beHost = beRow.get(1).toString().trim()
    String bePort =  beRow.get(4).toString().trim()
    logger.info("get be host and port:" + beHost + " " + bePort)

    //NOTE: this suite is used to check whether workload group's query queue works correctly when all query finished
    long beginTime = System.currentTimeMillis();
    long timeoutMs = 300 * 1000 // 300s
    boolean clear = false

    while ((System.currentTimeMillis() - beginTime) < timeoutMs) {
        List<List<Object>> result = sql "show workload groups;"
        logger.info("result, ${result}")

        clear = true
        for (int i = 0; i < result.size(); i++) {
            List<Object> row = result.get(i)
            int col_size = row.size()
            int running_query_num = Integer.valueOf(row.get(col_size - 2).toString())
            int waiting_query_num = Integer.valueOf(row.get(col_size - 1).toString())
            if (running_query_num != 0 || waiting_query_num != 0) {
                clear = false
                break
            }
        }

        if (clear) {
            break
        }

        Thread.sleep(500)
    }

    if (clear == false) {
        List<List<Object>> result = sql "select QUERY_ID,QUERY_START_TIME,QUERY_TIME_MS,WORKLOAD_GROUP_ID,SQL,QUERY_STATUS from information_schema.active_queries"
        logger.info("active queries: ${result}")
        if (result.size() == 1) {
            clear = true
        }

        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("curl http://" + beHost + ":" + bePort + "/api/running_pipeline_tasks/10")
        String command = strBuilder.toString()
        def process = command.toString().execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("Request be running task: code=" + code + ", err=" + err)
        logger.info("print be task:${out}")
    }

    assertTrue(clear)

    // check spill temporary files is cleared
    // Function to parse the metrics
    def getPrometheusMetric = { String data, String name -> {
        def metricValues = []
        data.eachLine { line ->
            line = line.trim()

            // Skip comment lines
            if (line.startsWith('#')) return

            // Regular expression to match metric lines
            def matcher = (line =~ /^(\w+)(\{[^}]+\})?\s+(.+)$/)

            if (matcher) {
                def metricName = matcher[0][1]
                def labels = matcher[0][2]
                def value = matcher[0][3]
                if (metricName == name) {
                    metricValues << value
                }
            }
        }
        return metricValues
    }

    }
    beginTime = System.currentTimeMillis();
    timeoutMs = 30 * 1000 // 30s
    clear = false

    def command = "curl http://${beHost}:${bePort}/metrics"
    while ((System.currentTimeMillis() - beginTime) < timeoutMs) {
        clear = true
        logger.info("executing command: ${command}")
        def process = command.execute()
        def outputStream = new StringBuffer()
        def errorStream = new StringBuffer()
        process.consumeProcessOutput(outputStream, errorStream)
        def code = process.waitFor()
        def metrics = outputStream.toString()
        logger.info("Request BE metrics: code=" + code + ", err=" + errorStream.toString())

        def hasSpillData = getPrometheusMetric(metrics, "doris_be_spill_disk_has_spill_data")
        logger.info("has spill temporary files :${hasSpillData}")
        for (int i = 0; i < hasSpillData.size(); i++) {
            if (0 != Integer.valueOf(hasSpillData.get(i))) {
                clear = false;
                break;
            }
        }

        hasSpillData = getPrometheusMetric(metrics, "doris_be_spill_disk_has_spill_gc_data")
        logger.info("has spill temporary files :${hasSpillData}")
        for (int i = 0; i < hasSpillData.size(); i++) {
            if (0 != Integer.valueOf(hasSpillData.get(i))) {
                clear = false;
                break;
            }
        }

        def spill_data_sizes = getPrometheusMetric(metrics, "doris_be_spill_disk_data_size")
        logger.info("spill data sizes :${spill_data_sizes}")
        for (int i = 0; i < spill_data_sizes.size(); i++) {
            if (0 != Integer.valueOf(spill_data_sizes.get(i))) {
                clear = false;
                break;
            }
        }

        if (clear) {
            break
        }

        Thread.sleep(2000)
    }
    assertTrue(clear)
}
