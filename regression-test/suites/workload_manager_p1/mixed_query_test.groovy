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
import groovy.sql.Sql
import org.apache.groovy.parser.antlr4.util.StringUtils

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import org.apache.commons.math3.stat.StatUtils

import java.util.concurrent.locks.ReentrantLock

/*

how to use:

third party dependency:
    1 need install java and groovy
    2 commons-math3-3.6.1.jar/mysql-connector-java-5.1.38.jar

command to run:
    groovy -cp "lib/*" mixed_query_test_conf.groovy
    lib contains commons-math3-3.6.1.jar/mysql-connector-java-5.1.38.jar

*/

def begin_time = System.currentTimeMillis()

ReentrantLock write_ret_lock = new ReentrantLock()
List<String> print_ret = new ArrayList<>()

def test_conf = new ConfigSlurper()
        .parse(
                new File("conf/mixed_query_test_conf.groovy")
                        .toURI()
                        .toURL()
        )


boolean enable_test = Boolean.parseBoolean(test_conf.global_conf.enable_test)
if (!enable_test) {
    System.exit(0)
}

url = test_conf.global_conf.url
username = test_conf.global_conf.username
password = test_conf.global_conf.password
boolean enable_pipe = Boolean.parseBoolean(test_conf.global_conf.enable_pipe)
boolean enable_group = Boolean.parseBoolean(test_conf.global_conf.enable_group)

AtomicBoolean should_stop = new AtomicBoolean(false);

def calculate_tpxx = { label, timecost_array, list ->
    List<Double> ret_val1 = new ArrayList<>();
    for (int[] array1 : timecost_array) {
        for (int val : array1) {
            ret_val1.add((double) val);
        }
    }

    double[] arr = ret_val1.toArray()
    double tp_50 = StatUtils.percentile(arr, 50)
    double tp_75 = StatUtils.percentile(arr, 75)
    double tp_90 = StatUtils.percentile(arr, 90)
    double tp_95 = StatUtils.percentile(arr, 95)
    double tp_99 = StatUtils.percentile(arr, 99)

    list.add(label + " tp50=" + tp_50)
    list.add(label + " tp75=" + tp_75)
    list.add(label + " tp90=" + tp_90)
    list.add(label + " tp95=" + tp_95)
    list.add(label + " tp99=" + tp_99)
}


def query_func = { conf ->
    AtomicInteger succ_num = new AtomicInteger(0)
    AtomicInteger failed_num = new AtomicInteger(0)
    long query_func_begin_time = System.currentTimeMillis()

    // 1 get sql list
    List<String> sql_array_list = new ArrayList<String>()
    def sql_file_dir = new File(conf.dir)
    File[] fs_list = sql_file_dir.listFiles()
    for (File sql_file : fs_list) {
        String[] sql_arr = sql_file.text.split(";")
        for (String sql :  sql_arr) {
            sql = sql.trim()
            if (StringUtils.isEmpty(sql)) {
                continue
            }
            sql_array_list.add(sql)
        }
    }

    List<Long> timeCost = new ArrayList<>()
    // 2 submit query
    int concurrency = Integer.parseInt(conf.c)
    int iteration = Integer.parseInt(conf.i)
    def threads = []
    for (int i = 0; i < concurrency; i++) {
        int curindex = i
        threads.add(Thread.startDaemon {
            def sql = Sql.newInstance(url, username, password, 'com.mysql.jdbc.Driver')
            if (enable_group && !StringUtils.isEmpty(conf.group)) {
                sql.execute("set workload_group='" + conf.group + "'")
            }
            if (enable_pipe) {
                sql.execute("set enable_pipeline_engine=true")
            } else {
                sql.execute("set enable_pipeline_engine=false")
            }
            if (!StringUtils.isEmpty(conf.db)) {
                sql.execute("use " + conf.db + ";")
            }
            for (int j = 0; j < iteration; j++) {
                if (should_stop.get()) {
                    break
                }

                for (int k = 0; k < sql_array_list.size(); k++) {
                    if (should_stop.get()) {
                        break
                    }

                    String query_sql = sql_array_list.get(k)
                    if (StringUtils.isEmpty(query_sql)) {
                        continue
                    }

                    def query_start_time = System.currentTimeMillis()
                    boolean is_succ = true;
                    try {
                        sql.execute(query_sql)
                        succ_num.incrementAndGet()
                    } catch (Exception e) {
                        is_succ = false;
                        failed_num.incrementAndGet()
                    }
                    if (!is_succ) {
                        continue
                    }
                    int query_time = System.currentTimeMillis() - query_start_time
                    println conf.label + " " + curindex + "," + j + "," + k + " : " + query_time + " ms"
                    timeCost.add(query_time)
                }
            }
        })
    }

    for (Thread t : threads) {
        t.join()
    }
    long query_func_timecost = System.currentTimeMillis() - query_func_begin_time;

    // 3 print test result
    write_ret_lock.lock()
    print_ret.add("\n")
    print_ret.add("=============" + conf.label + "=============")
    print_ret.add(conf.label + " iteration=" + iteration)
    print_ret.add(conf.label + " concurrency=" + concurrency)
    calculate_tpxx(conf.label, timeCost, print_ret)
    print_ret.add(conf.label + " succ sum=" + succ_num.get())
    print_ret.add(conf.label + " failed num=" + failed_num.get())
    print_ret.add(conf.label + " workload group=" + conf.group)
    print_ret.add(conf.label + " time cost=" + query_func_timecost)
    print_ret.add("==========================")

    write_ret_lock.unlock()
}

def t1 = Thread.start { query_func(test_conf.ckbench_query) }
def t2 = Thread.start { query_func(test_conf.tpch_query) }

t1.join()
t2.join()

for (int i = 0; i < print_ret.size(); i++) {
    println(print_ret.get(i))
}

def end_time = System.currentTimeMillis()

println "time cost=" + (end_time - begin_time)