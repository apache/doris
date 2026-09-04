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

import java.util.regex.Pattern

import org.apache.doris.regression.action.ProfileAction

// gram（稀疏 / 稠密 ngram）索引对 LIKE / REGEXP 的加速是「近似候选超集 + 表达式复验」：
// 索引只负责把候选行收窄，最终匹配仍由 LIKE / REGEXP 表达式逐行判定。因此本用例的核心断言是
// **语义对照**——同一条查询在 enable_inverted_index_query=true / false 两种模式下必须返回
// 完全相同的行集合。任何一处不一致都说明索引把真正匹配的行裁掉了（假阴性），属于功能缺陷。
//
// 两条兜底：
//   1) .out golden：每个模式各生成 idx_true_* / idx_false_* 两段，人工/脚本比对必须逐字节相同；
//   2) 程序化断言：runParityCheck 对同一批查询直接比较两种模式下排序后的 id 列表。
suite("test_gram_regexp_like", "p0") {
    def tbl = "t_gram_regexp_like"
    // 策略名全局唯一：tokenizer / analyzer 共享同一命名空间，两者名字必须不同
    def sparseTok = "gram_rl_sparse_tok"
    def sparseAna = "gram_rl_sparse"
    def denseLcTok = "gram_rl_dense_lc_tok"
    def denseLcAna = "gram_rl_dense_lc"

    // analyzer 通过心跳异步下发到 BE，建表前必须确认 BE 已装载，否则写入端拿不到 gram 方案
    def waitAnalyzerInstalled = { String name ->
        def deadline = System.currentTimeMillis() + 180_000
        Exception lastNotFound = null
        while (System.currentTimeMillis() < deadline) {
            try {
                sql """SELECT TOKENIZE('probe', '\"analyzer\"=\"${name}\"')"""
                return
            } catch (Exception e) {
                if (!e.message.contains("Policy not found")) {
                    throw e
                }
                lastNotFound = e
                sleep(1000)
            }
        }
        throw new IllegalStateException("analyzer ${name} was not installed on BE", lastNotFound)
    }

    // 把 groovy 侧的「正则/LIKE 原文」转成 SQL 字符串字面量内容：
    // Doris 与 MySQL 一样会在字符串字面量里再吃掉一层反斜杠，所以 \. 必须写成 \\.
    def sqlEsc = { String s -> s.replace('\\', '\\\\').replace("'", "\\'") }

    sql "DROP TABLE IF EXISTS ${tbl}"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${sparseAna}"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${denseLcAna}"
    try_sql "DROP INVERTED INDEX TOKENIZER IF EXISTS ${sparseTok}"
    try_sql "DROP INVERTED INDEX TOKENIZER IF EXISTS ${denseLcTok}"

    // 稀疏 gram：density=0.25，min_gram..max_gram = 3..16
    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS ${sparseTok}
        PROPERTIES (
            "type" = "ngram",
            "mode" = "sparse",
            "min_gram" = "3",
            "max_gram" = "16",
            "density" = "0.25"
        )
    """
    // gram 族 analyzer 只能是「纯 tokenizer」，不允许挂 token_filter（FE 校验）
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${sparseAna}
        PROPERTIES ("tokenizer" = "${sparseTok}")
    """
    // 稠密 gram + lower_case：用于覆盖大小写不敏感的候选召回（'code = unavailable' vs 'CODE = UNAVAILABLE'）
    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS ${denseLcTok}
        PROPERTIES (
            "type" = "ngram",
            "mode" = "dense",
            "min_gram" = "3",
            "lower_case" = "true"
        )
    """
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${denseLcAna}
        PROPERTIES ("tokenizer" = "${denseLcTok}")
    """
    waitAnalyzerInstalled(sparseAna)
    waitAnalyzerInstalled(denseLcAna)

    // 同一列上挂三个 INVERTED 索引：稀疏 gram / 稠密 gram（lower_case）/ english 分词索引。
    // 前两个用于验证「优化器挑哪个 gram 索引都必须保持语义一致」，第三个用于验证与语言分词索引共存。
    // gram 索引强制 docs-only，support_phrase 由 FE 缺省写成 false；存储格式必须是 SNII。
    sql """
        CREATE TABLE ${tbl} (
            id INT,
            msg VARCHAR(512),
            INDEX idx_msg_gram (msg) USING INVERTED PROPERTIES ("analyzer" = "${sparseAna}"),
            INDEX idx_msg_lc   (msg) USING INVERTED PROPERTIES ("analyzer" = "${denseLcAna}"),
            INDEX idx_msg_en   (msg) USING INVERTED PROPERTIES ("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "inverted_index_storage_format" = "SNII"
        )
    """

    // 第一个 rowset / segment
    sql """INSERT INTO ${tbl} VALUES
        (1, 'rpc error: code = Unavailable desc = error reading from server'),
        (2, 'user_id="eacb47f6-967d-11f0-b88d-8eb93cba8bdb" user_currency="USD"'),
        (3, 'Convert conversion successful'),
        (4, '手机微博 POST 10.68.3.18:8080 error'),
        (5, NULL),
        (6, ''),
        (7, 'ab'),
        (8, 'GET /images/x.gif HTTP/1.0'),
        (9, 'CODE = UNAVAILABLE'),
        (10, 'context deadline exceeded'),
        (11, 'failed to charge card: rpc error'),
        (12, 'timeout after error error error')"""
    sql "sync"
    // 第二个 rowset / segment：验证多 segment 下候选位图按 segment 分别求解仍然正确
    sql """INSERT INTO ${tbl} VALUES
        (13, 'rpc error: code = Internal desc = boom'),
        (14, '微博手机'),
        (15, 'abc'),
        (16, 'Sending Quote: 12.5'),
        (17, 'progress 100% done'),
        (18, '   '),
        (19, 'MiXeD CaSe UnAvAiLaBlE')"""
    sql "sync"

    // 覆盖矩阵：长字面量 / 通配 / 交替 / 可选组 / 字符类 / 无字面量 / 锚点 / 转义点 /
    // 大小写（(?i) 与 lower_case 索引）/ CJK / 短于 min_gram 的字面量 / 空模式
    def regexps = [
        'rpc error: code = Unavailable',
        'error.*timeout',
        'code = (Unavailable|Internal)',
        'user_id="[0-9a-f]{8}-',
        'conn(ection)? re(set|fused)',
        'GET|POST',
        '[0-9]{3}-[0-9]{4}',
        '(?i)unavailable',
        '手机微博',
        '微博',
        '^abc$',
        'a.*b',
        'Sending Quote: [0-9]+\\.[0-9]+',
        'failed to (convert|charge)',
        '.*',
        '[0-9]+',
        'code = unavailable',
        'error',
        '\\.gif',
        '^rpc',
        'exceeded$',
        'ab',
        'x',
        '',
        '用户|微博',
        'HTTP/1\\.[0-9]',
    ]

    // LIKE 覆盖：% / _ 通配、空串、精确匹配、多段通配、CJK、大小写
    def likes = [
        '%rpc error%',
        '%Unavail%',
        '%手机%',
        'ab%',
        '%',
        '%x.gif%',
        '%code = _navailable%',
        '',
        'abc',
        '%error%error%',
        '_bc',
        '%微博%',
        '%CODE = UNAVAILABLE%',
    ]

    // 复合谓词：REGEXP 的 OR、NOT (a AND b) 里的 REGEXP、REGEXP 与 LIKE 混用，
    // 以及自定义 ESCAPE 的 LIKE（BE 侧保守处理：不下推也必须正确）
    def compounds = [
        "msg REGEXP 'rpc' OR msg REGEXP '微博'",
        "NOT (msg REGEXP 'rpc' AND id > 5)",
        "msg REGEXP 'error' AND id > 3",
        "msg LIKE '%error%' AND msg REGEXP 'rpc'",
        "NOT (msg REGEXP 'error') OR msg REGEXP 'abc'",
        "NOT (msg LIKE '%rpc%' AND msg REGEXP 'error')",
        "msg REGEXP 'code' AND msg NOT LIKE '%Internal%'",
        "msg LIKE '%100!%%' ESCAPE '!'",
        "msg LIKE '%100!% do%' ESCAPE '!'",
    ]

    // 生成 [tag, sql] 列表；tag 不含 pattern 原文，保证 .out 的段名稳定可复现
    def buildQueries = { ->
        def qs = []
        regexps.eachWithIndex { p, i ->
            def e = sqlEsc(p)
            qs << ["regexp_${i}".toString(),
                   "SELECT id FROM ${tbl} WHERE msg REGEXP '${e}'".toString()]
            qs << ["rlike_${i}".toString(),
                   "SELECT id FROM ${tbl} WHERE msg RLIKE '${e}'".toString()]
            qs << ["notregexp_${i}".toString(),
                   "SELECT id FROM ${tbl} WHERE msg NOT REGEXP '${e}'".toString()]
            qs << ["regexp_and_${i}".toString(),
                   "SELECT id FROM ${tbl} WHERE msg REGEXP '${e}' AND id > 3".toString()]
        }
        likes.eachWithIndex { p, i ->
            def e = sqlEsc(p)
            qs << ["like_${i}".toString(),
                   "SELECT id FROM ${tbl} WHERE msg LIKE '${e}'".toString()]
            qs << ["notlike_${i}".toString(),
                   "SELECT id FROM ${tbl} WHERE msg NOT LIKE '${e}'".toString()]
        }
        compounds.eachWithIndex { c, i ->
            qs << ["compound_${i}".toString(),
                   "SELECT id FROM ${tbl} WHERE ${c}".toString()]
        }
        return qs
    }

    def queries = buildQueries()
    log.info("gram parity matrix: ${queries.size()} queries x 2 modes".toString())

    // 关闭 SQL cache，避免两种模式复用同一份缓存结果导致对照失真
    sql "SET enable_sql_cache=false"

    // 生成 .out golden：同一批查询分别在索引开 / 关两种模式下各跑一遍
    def runAll = { boolean useIndex ->
        sql "SET enable_inverted_index_query=${useIndex}"
        queries.each { entry ->
            def tag = "${entry[0]}_idx_${useIndex}".toString()
            "order_qt_${tag}"(entry[1])
        }
        "order_qt_count_idx_${useIndex}"("SELECT count(*) FROM ${tbl} WHERE msg REGEXP 'error'".toString())
    }
    runAll(true)
    runAll(false)

    // 程序化兜底：不依赖 .out 目测，逐条比较两种模式下排序后的 id 列表
    def idsOf = { String q ->
        return sql(q).collect { it[0] as Integer }.sort()
    }
    def runParityCheck = { String phase ->
        def mismatches = []
        queries.each { entry ->
            sql "SET enable_inverted_index_query=true"
            def withIdx = idsOf(entry[1])
            sql "SET enable_inverted_index_query=false"
            def noIdx = idsOf(entry[1])
            if (withIdx != noIdx) {
                mismatches << "[${phase}][${entry[0]}] ${entry[1]}\n  idx_on =${withIdx}\n  idx_off=${noIdx}".toString()
            }
        }
        if (!mismatches.isEmpty()) {
            throw new AssertionError(
                    "gram index changed query semantics (${mismatches.size()} mismatches):\n"
                            + mismatches.join("\n") as Object)
        }
        log.info("gram parity check [${phase}] passed for ${queries.size()} queries".toString())
    }
    runParityCheck("base")

    // profile 证明 gram 索引真的参与了裁剪：RowsGramIndexFiltered > 0。
    // 计数器可能被渲染成 "18" 或 "12.0K (12000)"，两种形态都要能解析。
    def parseProfileCounter = { String profileString, String name ->
        def exact = Pattern.compile(Pattern.quote(name) + ":\\s*[^\\(\\n]*\\((\\d+)\\)").matcher(profileString)
        if (exact.find()) {
            return Long.parseLong(exact.group(1))
        }
        def plain = Pattern.compile(Pattern.quote(name) + ":\\s*(\\d+)").matcher(profileString)
        assertTrue(plain.find(), "${name} is not parseable from profile")
        return Long.parseLong(plain.group(1))
    }
    def checkGramPruned = { String label, String profileString, Throwable exception ->
        assertNull(exception)
        assertTrue(profileString.contains("RowsGramIndexFiltered"),
                "RowsGramIndexFiltered is missing from profile")
        def filtered = parseProfileCounter(profileString, "RowsGramIndexFiltered")
        def candidate = parseProfileCounter(profileString, "GramIndexCandidateRows")
        log.info("[${label}] RowsGramIndexFiltered=${filtered}, GramIndexCandidateRows=${candidate}".toString())
        assertTrue(filtered > 0, "[${label}] RowsGramIndexFiltered must be positive, got ${filtered}")
    }

    sql "SET enable_inverted_index_query=true"
    sql "set enable_profile=true"
    sql "set profile_level=2"
    // profile 由 FE 异步汇报，固定 sleep 要么白等要么在慢机器上偶发抓空。改用框架自带的有界
    // 轮询 ProfileAction#getProfileBySql：最多等 30 s（每 500 ms 一次），直到该 SQL 的 profile
    // 变成 "Profile Completion State: COMPLETE" 且两个 gram 计数器都已渲染出来；超时即失败。
    def gramProfileCounters = ["RowsGramIndexFiltered", "GramIndexCandidateRows"]
    def profileAction = new ProfileAction(context)
    // REGEXP 走 gram 加速
    order_qt_profile_q "/* gram_regexp_profile */ SELECT id FROM ${tbl} WHERE msg REGEXP 'context deadline exceeded'"
    checkGramPruned("regexp",
            profileAction.getProfileBySql("gram_regexp_profile", gramProfileCounters), null)
    // LIKE 同样走 gram 加速
    order_qt_profile_q_like "/* gram_like_profile */ SELECT id FROM ${tbl} WHERE msg LIKE '%Sending Quote%'"
    checkGramPruned("like",
            profileAction.getProfileBySql("gram_like_profile", gramProfileCounters), null)
    sql "set enable_profile=false"

    // 删除后再查：delete 谓词与 gram 候选位图叠加后仍必须与不走索引一致
    sql "SET enable_inverted_index_query=true"
    sql "DELETE FROM ${tbl} WHERE id = 1"
    sql "sync"
    order_qt_after_delete_idx_true "SELECT id FROM ${tbl} WHERE msg REGEXP 'rpc error'"
    sql "SET enable_inverted_index_query=false"
    order_qt_after_delete_idx_false "SELECT id FROM ${tbl} WHERE msg REGEXP 'rpc error'"
    // 删除之后整个矩阵再对照一遍
    runParityCheck("after_delete")

    sql "SET enable_inverted_index_query=true"
}
