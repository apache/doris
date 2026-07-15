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

Suite.metaClass.advance_ivm_stream_offset = { String mtmvName ->
    def suite = delegate
    def dbName = suite.context.dbName
    def tmpTableRows = suite.sql("""show tables from ${dbName} like 'test_ivm_tmp'""")
    if (tmpTableRows.isEmpty()) {
        suite.sql("""
            CREATE TABLE IF NOT EXISTS ${dbName}.test_ivm_tmp (
                v INT
            )
            DUPLICATE KEY(v)
            DISTRIBUTED BY HASH(v) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """)
    }
    suite.sql("""truncate table ${dbName}.test_ivm_tmp""")
    def mvRows = suite.sql("""
        select Id, QuerySql
        from mv_infos('database'='${dbName}')
        where Name = '${mtmvName}'
    """)
    assert mvRows.size() == 1 : "Expected exactly one MV named ${mtmvName}, but got ${mvRows.size()}"

    def mvId = mvRows[0][0].toString()
    def querySql = mvRows[0][1].toString()
    def baseTableNames = [] as LinkedHashSet<String>
    def matcher = querySql =~ ~/(?i)\b(?:FROM|JOIN)\s+`[^`]+`\.`[^`]+`\.`([^`]+)`/
    matcher.each { match ->
        baseTableNames.add(match[1])
    }
    assert !baseTableNames.isEmpty() : "No base tables found in MV query for ${mtmvName}: ${querySql}"

    baseTableNames.each { baseTableName ->
        def streamName = "__doris_ivm_stream_${mvId}_${baseTableName}"
        suite.sql("""
            insert into ${dbName}.test_ivm_tmp
            select 1 from `${streamName}`
        """)
    }
}
