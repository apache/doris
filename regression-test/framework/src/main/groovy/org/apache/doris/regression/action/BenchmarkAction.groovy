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

package org.apache.doris.regression.action

import groovy.util.logging.Slf4j
import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.util.JdbcUtils
import org.apache.doris.regression.util.SuiteUtils

import java.math.RoundingMode

@Slf4j
class BenchmarkAction implements SuiteAction {
    private int executeTimes = 3
    private boolean skipFailure = true
    private boolean printResult = true
    private boolean warmUp = true
    private List<String> sqls

    SuiteContext context

    BenchmarkAction(SuiteContext context) {
        this.context = context
    }

    void sqls(String... sqls) {
        this.sqls = sqls.toList()
    }

    void sqls(List<String> sqls) {
        this.sqls = sqls
    }

    void executeTimes(int times) {
        this.executeTimes = times
    }

    void skipFailure(boolean skipFailure) {
        this.skipFailure = skipFailure
    }

    void warmUp(boolean warmUp) {
        this.warmUp = warmUp
    }

    void printResult(boolean printResult) {
        this.printResult = printResult
    }

    @Override
    void run() {
        if (warmUp) {
            log.info("start to warm up")
            for (int i = 1; i <= sqls.size(); ++i) {
                def sql = sqls[i - 1]
                log.info("Start to warm up sql ${i}:\n${sql}".toString())

                try {
                    JdbcUtils.executeToList(context.getConnection(), sql)
                } catch (Throwable t) {
                    if (!skipFailure) {
                        throw t
                    }
                }
            }
        }

        log.info("start to run benchmark")
        List<Map<String, Double>> results = []
        for (int i = 1; i <= sqls.size(); ++i) {
            try {
                def sql = sqls[i - 1]
                log.info("Start to execute sql ${i}:\n${sql}".toString())

                List<Long> elapsedInfo = []
                for (int times = 1; times <= executeTimes; ++times) {
                    log.info("Execute sql ${i} for the ${times == 1 ? "first" : times} time${times > 1 ? "s" : ""}".toString())
                    def (_, elapsed) = SuiteUtils.timer {
                        JdbcUtils.executeToList(context.getConnection(), sql)
                    }
                    elapsedInfo.add(elapsed)
                }

                def avg = avg(elapsedInfo)
                def min = min(elapsedInfo)
                def max = max(elapsedInfo)

                results.add([min: min, max: max, avg: avg])
                log.info("Execute sql ${i} result: avg: ${avg} ms, min: ${min} ms, max: ${max} ms".toString())
            } catch (Throwable t) {
                if (!skipFailure) {
                    throw t
                }
            }
        }

        if (printResult) {
            String line = "+-----------+---------------+---------------+---------------+\n"
            String resultStrings = line + "|  SQL ID   |      avg      |      min      |      max      |\n" + line
            List<Double> avgResults = []
            List<Long> minResults = []
            List<Long> maxResults = []
            for (int i = 1; i <= results.size(); ++i) {
                def result = results[i - 1]
                resultStrings += String.format("|  SQL %-3d  | %10.2f ms | %10d ms | %10d ms |\n",
                        i, result["avg"], result["min"], result["max"])
                avgResults.add(result["avg"])
                minResults.add(result["min"].toLong())
                maxResults.add(result["max"].toLong())
            }
            resultStrings += line +
                    String.format("| TOTAL AVG | %10.2f ms | %10.2f ms | %10.2f ms |\n",
                            avg(avgResults), avg(minResults), avg(maxResults)) +
                    String.format("| TOTAL SUM | %10.2f ms | %10d ms | %10d ms |\n",
                            sum(avgResults), sum(minResults), sum(maxResults)) + line
            log.info("bechmark result: \n${resultStrings}")
        }
    }

    private Number max(List<Long> numbers) {
        return numbers.stream()
            .reduce{ n1, n2 -> return Math.max(n1, n2) }.orElse(0L)
    }

    private Number min(List<Number> numbers) {
        return numbers.stream()
                .reduce{ n1, n2 -> return Math.min(n1, n2) }.orElse(0L)
    }

    private Number sum(List<Number> numbers) {
        return numbers.stream()
                .reduce{ n1, n2 -> return n1 + n2 }.orElse(0L)
    }

    private double avg(List<Number> numbers) {
        double result = numbers.isEmpty() ? 0 : sum(numbers) * 1.0 / numbers.size()
        return new BigDecimal(result).setScale(2, RoundingMode.HALF_UP).toDouble()
    }
}
