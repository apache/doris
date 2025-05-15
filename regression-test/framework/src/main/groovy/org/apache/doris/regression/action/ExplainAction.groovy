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

import groovy.transform.stc.ClosureParams
import groovy.transform.stc.FromString
import org.apache.doris.regression.suite.SuiteContext
import org.apache.doris.regression.util.JdbcUtils
import groovy.util.logging.Slf4j
import java.sql.ResultSetMetaData
import java.util.regex.Pattern
import java.util.stream.Collectors

@Slf4j
class ExplainAction implements SuiteAction {
    private String sql
    private boolean verbose = false
    private List<String> checkSlots = new ArrayList<>()
    private List<String> expectedSlotTypes = new ArrayList<>()
    private SuiteContext context
    private Set<String> containsStrings = new LinkedHashSet<>()
    private Set<String> containsAnyStrings = new LinkedHashSet<>()
    private Set<String> notContainsStrings = new LinkedHashSet<>()
    private Map<String, Integer> multiContainsStrings = new HashMap<>()
    private String coonType
    private Closure checkFunction

    ExplainAction(SuiteContext context, String coonType = "JDBC") {
        this.context = context
        this.coonType = coonType
    }

    void sql(String sql) {
        this.sql = sql
    }

    void verbose(boolean verbose) {
        this.verbose = verbose
    }

    void sql(Closure<String> sqlSupplier) {
        this.sql = sqlSupplier.call()
    }

    void checkSlotTypeOf(String checkSlot, String expectedSlotType) {
        this.verbose = true
        this.checkSlots.add(checkSlot)
        this.expectedSlotTypes.add(expectedSlotType)
        if (checkSlots.size() != expectedSlotTypes.size()) {
            throw new IllegalStateException("checkSlots and expectedSlotTypes size not equal")
        }
    }

    void contains(String subString) {
        containsStrings.add(subString)
    }

    void containsAny(String subString) {
        containsAnyStrings.add(subString)
    }

    void multiContains(String subString, int n) {
        multiContainsStrings.put(subString, n);
    }

    void notContains(String subString) {
        notContainsStrings.add(subString)
    }

    void check(@ClosureParams(value = FromString, options = ["String", "String,Throwable,Long,Long"]) Closure<Boolean> checkFunction) {
        this.checkFunction = checkFunction
    }

    @Override
    void run() {
        String explainSql = "explain\n" + (verbose ? "verbose\n" : "") + sql
        def result = doTest(explainSql)
        String explainString = result.result

        if (checkSlots.size() > 0) {
            assertTrue(explainString != null, "Explain failed")
            checkSlotType(explainString)
        }
        if (checkFunction != null) {
            try {
                Boolean checkResult = null
                if (checkFunction.parameterTypes.size() == 1) { 
                    if (result.exception == null) {
                        checkResult = checkFunction(explainString)
                    } else {
                        throw result.exception
                    }
                } else {
                    checkResult = checkFunction(explainString, result.exception, result.startTime, result.endTime)
                }
                if (checkResult != null && checkResult.booleanValue() == false) {
                    String msg = "Explain and custom check failed, actual explain string is:\n${explainString}".toString()
                    throw new IllegalStateException(msg)
                }
            } catch (Throwable t) {
                log.error("Explain and custom check failed", t)
                throw t
            }
        } else if (result.exception != null) {
            String msg = "Explain failed"
            log.error(msg, result.exception)
            throw new IllegalStateException(msg, result.exception)
        } else {
            for (String string : containsStrings) {
                if (!explainString.contains(string)) {
                    String msg = ("Explain and check failed, expect contains '${string}',"
                            + " but actual explain string is:\n${explainString}").toString()
                    def t = new IllegalStateException(msg)
                    throw t
                }
            }
            for (String string : notContainsStrings) {
                if (explainString.contains(string)) {
                    String msg = ("Explain and check failed, expect not contains '${string}',"
                            + " but actual explain string is:\n${explainString}").toString()
                    def t = new IllegalStateException(msg)
                    throw t
                }
            }
            for (Map.Entry entry : multiContainsStrings) {
                int count = explainString.count(entry.key);
                if (count != entry.value) {
                    String msg = ("Explain and check failed, expect multiContains '${entry.key}' , '${entry.value}' times, actural '${count}' times."
                            + "Actual explain string is:\n${explainString}").toString()
                    def t = new IllegalStateException(msg)
                    throw t
                }
            }
            boolean any = false;
            for (String string : containsAnyStrings) {
                if (explainString.contains(string)) {
                    any = true;
                }
            }
            if (!containsAnyStrings.isEmpty() && !any) {
                    String msg = ("Explain and check failed, expect contains any '${containsAnyStrings}',"
                            + " but actual explain string is:\n${explainString}").toString()
                    def t = new IllegalStateException(msg)
                    throw t
            }
        }
    }

    private void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message)
        }
    }

    private ActionResult doTest(String explainSql) {
        log.info("Execute sql:\n${explainSql}".toString())
        long startTime = System.currentTimeMillis()
        String explainString = null
        ResultSetMetaData meta = null
        try {
            def temp = null
            if (coonType == "JDBC") {
                (temp, meta) = JdbcUtils.executeToList(context.getConnection(), explainSql)
            } else if (coonType == "ARROW_FLIGHT_SQL") {
                (temp, meta) = JdbcUtils.executeToList(context.getArrowFlightSqlConnection(), (String) ("USE ${context.dbName};" + explainSql))
            }
            explainString = temp.stream().map({row -> row.get(0).toString()}).collect(Collectors.joining("\n"))
            return new ActionResult(explainString, null, startTime, System.currentTimeMillis(), meta)
        } catch (Throwable t) {
            return new ActionResult(explainString, t, startTime, System.currentTimeMillis(), meta)
        }
    }

    private String getSlotId(List<String> explainResult, String exprStr) {
        String matchLineStr = explainResult.find { it =~ Pattern.quote(exprStr) + /\[#(\d+)\]/ }
        if (matchLineStr) {
            def matcher = matchLineStr =~ Pattern.quote(exprStr) + /\[#(\d+)\]/
            assert matcher.find()
            return matcher.group(1)
        } else {
            assertTrue(false, "not found expr ${exprStr} with slot number in explain result")
        }
    }

    private static String getType(String input) {
        def matcher = input =~ /type=([^,]*),/
        if (matcher.find()) {
            return matcher.group(1)
        } else {
            assertTrue(false, "not found type in SlotDescriptor")
        }
    }

    private checkSlotType(String originExplainResult) {
        // get explain result strings
        List<String> explainResult = originExplainResult.split("\n")
        for (int i = 0; i < checkSlots.size(); i++) {
            String checkSlot = checkSlots.get(i)
            String expectedSlotType = expectedSlotTypes.get(i)
            checkSlotTypeOneRun(explainResult, checkSlot, expectedSlotType)
        }
    }

    private checkSlotTypeOneRun(List<String> explainResult, String checkSlot, String expectedSlotType) {
        // get slot id for checkSlot expr string
        String slotId = getSlotId(explainResult, checkSlot)
        // get slot descriptor line like "SlotDescriptor{id=1, col=...
        String slotDescriptor = explainResult.find { it.contains("SlotDescriptor{id=${slotId}") }
        // extract X from "SlotDescriptor{id=1, col=..., type=X, ...}"
        String type = getType(slotDescriptor)
        assertTrue(type == expectedSlotType, "expect type ${expectedSlotType}, but actual type is ${type}")
    }

    class ActionResult {
        String result
        Throwable exception
        long startTime
        long endTime
        ResultSetMetaData meta

        ActionResult(String result, Throwable exception, long startTime, long endTime, ResultSetMetaData meta) {
            this.result = result
            this.exception = exception
            this.startTime = startTime
            this.endTime = endTime
            this.meta = meta
        }
    }
}
