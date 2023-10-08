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
import org.apache.commons.math3.stat.StatUtils
import org.apache.groovy.parser.antlr4.util.StringUtils

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/*

how to use:

third party dependency:
    1 need install java and groovy
    2 commons-math3-3.6.1.jar/mysql-connector-java-5.1.38.jar

command to run:
    groovy -cp "lib/*" basic_workload_group_test.groovy
    lib contains commons-math3-3.6.1.jar/mysql-connector-java-5.1.38.jar

data:
    default sql and data comes from clickbench

*/


def begin_time = System.currentTimeMillis()

def url = 'jdbc:mysql://127.0.0.1:9030/hits?useSSL=false'
def username = 'root'
def password = ''

AtomicBoolean should_stop = new AtomicBoolean(false);
AtomicInteger bigq_succ_num = new AtomicInteger(0);
AtomicInteger bigq_failed_num = new AtomicInteger(0);

AtomicInteger smallq_succ_num = new AtomicInteger(0);
AtomicInteger smallq_failed_num = new AtomicInteger(0);

AtomicInteger concurrency_succ_num = new AtomicInteger(0);
AtomicInteger concurrency_failed_num = new AtomicInteger(0);

def query_func = { sql, label, test_sql, time_array, succ_num, failed_num ->
    def start_time = System.currentTimeMillis()
    String err_msg = ""
    boolean is_succ = true
    try {
        sql.execute(test_sql)
        succ_num.incrementAndGet()
    } catch (Exception e) {
        failed_num.incrementAndGet()
        err_msg = e.getMessage()
        is_succ = false
    }
    if (!is_succ) {
        return
    }
    def end_time = System.currentTimeMillis()
    def exec_time = end_time - start_time
    time_array.add(exec_time)
    println(label + " : " + exec_time)
    println()
}

// label, test name
// group name, workload group name
// test_sql, sql
// file_name, the file contains sql, if file_name and test_sql are both not empty, then file_name works
// concurrency, how many threads to send query at the same time
// iterations, how many times to send query to doris
// time_array, save query time
def thread_query_func = { label, group_name, test_sql, file_name, concurrency, iterations, time_array, succ_num, failed_num  ->
    def threads = []
    def cur_sql = test_sql
    if (!StringUtils.isEmpty(file_name)) {
        def q_file = new File(file_name)
        cur_sql = q_file.text
    }

    for (int i = 0; i < concurrency; i++) {
        def cur_array = []
        time_array.add(cur_array);
        threads.add(Thread.startDaemon {
            def sql = Sql.newInstance(url, username, password, 'com.mysql.jdbc.Driver')
            if (group_name != "") {
                sql.execute("set workload_group='" + group_name + "'")
            }
            for (int j = 0; j < iterations; j++) {
                if (should_stop.get()) {
                    break
                }
                query_func(sql, label + " " + j, cur_sql, cur_array, succ_num, failed_num)
            }
        })
    }

    for (Thread t in threads) {
        t.join()
    }
    println(label + " query finished")
    should_stop.set(true)
}

def calculate_tpxx = { label, timecost_array ->
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

    println(label + " tp50=" + tp_50)
    println(label + " tp75=" + tp_75)
    println(label + " tp90=" + tp_90)
    println(label + " tp95=" + tp_95)
    println(label + " tp99=" + tp_99)
}

def print_test_result = { label, c, i, time_cost, succ_num, failed_num ->
    println label + " iteration=" + i
    println label + " concurrency=" + c
    calculate_tpxx(label, time_cost)
    println label + " succ sum=" + succ_num.get()
    println label + " failed num=" + failed_num.get()
    println ""
}



def test_two_group_query = {
    def bigquery = 'SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits.hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;'
    int bigquery_c = 1
    int bigquery_i = 1
    def bigquery_timecost = [][]
    def bigquery_group_name = ""
    def bigquery_file = ""
    def test_label_1 = "bigq"
    def bigquery_thread = Thread.start {
        thread_query_func(test_label_1, bigquery_group_name, bigquery, bigquery_file, bigquery_c, bigquery_i, bigquery_timecost, bigq_succ_num, bigq_failed_num);
    }

    def smallquery = 'SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits.hits'
    int smallquery_c = 1
    int smallquery_i = 10
    def smallquery_timecost = [][]
    def small_group_name = ""
    def small_query_file = ""
    def test_label_2 = "smallq"
    def smallquery_thread = Thread.start {
        thread_query_func(test_label_2, small_group_name, smallquery, small_query_file, smallquery_c, smallquery_i, smallquery_timecost, smallq_succ_num, smallq_failed_num);
    }

    bigquery_thread.join()
    smallquery_thread.join()

    println ""
    print_test_result(test_label_1, bigquery_c, bigquery_i, bigquery_timecost, bigq_succ_num, bigq_failed_num)
    print_test_result(test_label_2, smallquery_c, smallquery_i, smallquery_timecost, smallq_succ_num, smallq_failed_num)

}

def test_concurrency = {
    def test_label = "concurrency"
    def group_name = ""
    def query = 'SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits.hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;'
    def query_file = ""
    int c = 1
    int i = 10
    def timecost = [][]
    def test_thread = Thread.start {
        thread_query_func(test_label, group_name, query, query_file, c, i, timecost, concurrency_succ_num, concurrency_failed_num);
    }
    test_thread.join()

    println ""
    print_test_result(test_label, c, i, timecost, concurrency_succ_num, concurrency_failed_num)
}

def show_global_config = {
    println "========== show global config info"
    def show_sql_con = Sql.newInstance(url, username, password, 'com.mysql.jdbc.Driver')
    def show_sql1 = "show variables like '%experimental_enable_pipeline_engine%'"
    def show_sql2 = "ADMIN SHOW FRONTEND CONFIG like '%enable_workload_group%';"
    def show_sql3 = "show variables like '%parallel_fragment_exec_instance_num%';"
    def show_sql4 = "show variables like '%parallel_pipeline_task_num%';"
    show_sql_con.eachRow(show_sql1,) { row ->
        println row[0] + " = " + row[1]
    }

    show_sql_con.eachRow(show_sql2,) { row ->
        println row[0] + " = " + row[1]
    }

    show_sql_con.eachRow(show_sql3,) { row ->
        println row[0] + " = " + row[1]
    }

    show_sql_con.eachRow(show_sql4,) { row ->
        println row[0] + " = " + row[1]
    }
}

// note(wb) you can close the comment to test

// test 1, test two group runs at same time
//test_two_group_query()

// test2, just run one group to test concurrency
//test_concurrency()

// show config
//show_global_config()

//println "==========Test finish, time cost=" + (System.currentTimeMillis() - begin_time) / 1000