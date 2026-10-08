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

import org.apache.doris.regression.suite.Suite

def anlyzedPlanShowFields = [
    LogicalProject : ['projects'],
    LogicalSort : ['orderKeys'],
    LogicalFilter : ['predicates'],
    LogicalHaving : ['predicates'],
    LogicalQualify : ['predicates'],
    LogicalAggregate : ['groupByExpr', 'outputExpr'],
    LogicalOlapScan : ['qualified'],
    LogicalResultSink : ['outputExprs'],
    LogicalJoin : ['type', 'markJoinSlotReference', 'hashJoinConjuncts', 'otherJoinConjuncts', 'markJoinConjuncts'],
]

def extractAttributes(String input) {
    // 正则解释：
    // (\w+)           - 匹配key
    // =               - 等号
    // (.+?)           - 非贪婪匹配value（任意字符）
    // (?=             - 正向向前查找
    //   \s*,\s*\w+=   - 后面是逗号+空格+下一个key=
    //   |             - 或
    //   \s*\)         - 后面是空格+右括号
    //   |             - 或
    //   $             - 字符串结束
    // )
    // def pattern = /(\w+)=(.+?)(?=\s*,\s*\w+=|\s*\)|$)/
    def pattern = /(\w+)=(.+?)(?=\s*,\s*\w+=|\s\)|$)/
    def matcher = input =~ pattern
    def result = [:]

    matcher.each { match ->
        result[match[1]] = match[2].trim()
    }

    return result
}

def convertPlanRow = { row, showFieldMap ->
    def cell = row[0]
    def s = ""
    for (def line : cell.split("\n")) {
        if (!s.isEmpty()) {
            s += "\n"
        }
        def p1 = line.findIndexOf { Character.isLetter(it as char) }
        if (p1 == -1) {
            s += line
            continue
        }
        def p2 = p1 + line.substring(p1).findIndexOf { !Character.isLetter(it as char) }
        if (p2 < p1) {
            s += line
            continue
        }
        def planName = line.substring(p1, p2)
        def showFields = showFieldMap.getOrDefault(planName, [])
        s += line.substring(0, p2)
        s += " ( "
        def showAttrs = []
        def attrs = extractAttributes(line)
        for (def field : showFields) {
            if (attrs.containsKey(field)) {
                showAttrs.add("${field}=${attrs[field]}".toString())
            }
        }
        s += showAttrs.join(", ")
        s += " )"
    }
    return [s]
}

def convertIvmExplainRow = { row ->
    def planIndex = row.size() - 1
    def plan = row[planIndex].toString()
            .replaceAll(/#\d+/, "#")
            .replaceAll(/\b([A-Za-z][A-Za-z0-9_]*)\[\d+\]/, '$1[]')
            .replaceAll(/selectedIndexId=[^,\s\)]+/, "selectedIndexId=<id>")
    def converted = []
    for (int i = 0; i < row.size(); i++) {
        converted.add(i == planIndex ? plan : row[i].toString())
    }
    return converted
}

Suite.metaClass.explainAndResult = { String tag, String sql ->
    "qt_${tag}_shape"          "explain shape plan ${sql}"
    "qt_${tag}_result"         "${sql}"
}

Suite.metaClass.explainAndOrderResult = { String tag, String sql ->
    "qt_${tag}_shape"          "explain shape plan ${sql}"
    "order_qt_${tag}_result"   "${sql}"
}

Suite.metaClass.explainAnalyzedPlan = { String tag, String sql,  overwriteShowFields = [:] ->
    def showFields = anlyzedPlanShowFields
    if (!overwriteShowFields.isEmpty()) {
        showFields = [:]
        showFields.putAll(anlyzedPlanShowFields)
        showFields.putAll(overwriteShowFields)
    }

    delegate.quickRunTest(
            tag,
            "explain analyzed plan ${sql}".toString(),
            false,
            {row -> convertPlanRow(row, showFields) }
            )
}

Suite.metaClass.explainIvmPlan = { String tag, String sql ->
    delegate.quickRunTest(
            tag,
            sql,
            false,
            convertIvmExplainRow
            )
}

// __DORIS_SEQUENCE_COL__ of an IVM dry-run delta encodes the refresh version, whose base value
// differs between cloud and shared-nothing deployments. Mask that column so that both modes can
// share one suite and one .out file.
def maskIvmDryRunSequence = { row, meta ->
    for (int i = 1; i <= meta.getColumnCount(); i++) {
        if ("__DORIS_SEQUENCE_COL__".equalsIgnoreCase(meta.getColumnLabel(i))) {
            def masked = new ArrayList(row)
            masked.set(i - 1, "[regression-fake-sequence]")
            return masked
        }
    }
    return row
}

// Named ivm_dry_run_qt instead of order_qt_*: Suite.invokeMethod intercepts every method whose
// name starts with qt_ / order_qt_ and derives the tag from the method name, so a metaClass
// method could never be reached under those prefixes.
Suite.metaClass.ivm_dry_run_qt = { String tag, String sql ->
    delegate.quickRunTest(tag, sql, true, maskIvmDryRunSequence)
}

