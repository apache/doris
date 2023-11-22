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

package org.apache.doris.rewrite;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableMap;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.function.Function;

/*
 * rewrite sql: select * from table where from_unixtime(query_time) > '2021-03-02 12:12:23'
 * into: select * from table where query_time > 1614658343
 * query_time is big integer type to keep consistent with mysql
 * 1614658343 is the timestamp of 2021-03-02 12:12:23
 * this rewrite can improve the query performance, because from_unixtime is cpu-exhausted
 * */
public class RewriteFromUnixTimeRule implements ExprRewriteRule {

    public static RewriteFromUnixTimeRule INSTANCE = new RewriteFromUnixTimeRule();
    // In BE, will convert format in timestamp function.
    // yyyyMMdd -> %Y%m%d
    // yyyy-MM-dd -> %Y-%m-%d
    // yyyy-MM-dd HH:mm:ss -> %Y-%m-%d %H:%i:%s
    // Here, we just support these three format.
    private final ImmutableMap<String, String> beSupportFormatMap;
    private final ImmutableMap<String, Function<String, Long>> parseMillisFunctionMap;

    public RewriteFromUnixTimeRule() {
        beSupportFormatMap = ImmutableMap.<String, String>builder()
                .put("%Y%m%d", "yyyyMMdd")
                .put("%Y-%m-%d", "yyyy-MM-dd")
                .put("%Y-%m-%d %H:%i:%s", "yyyy-MM-dd HH:mm:ss")
                .build();
        parseMillisFunctionMap = ImmutableMap.<String, Function<String, Long>>builder()
                .put("yyyyMMdd", (str) -> LocalDate.parse(str, DateTimeFormatter.ofPattern("yyyyMMdd")).atStartOfDay()
                        .toEpochSecond(OffsetDateTime.now().getOffset()))
                .put("yyyy-MM-dd", (str) -> LocalDate.parse(str, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                        .atStartOfDay().toEpochSecond(OffsetDateTime.now().getOffset()))
                .put("yyyy-MM-dd HH:mm:ss",
                        (str) -> LocalDateTime.parse(str, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                                .toEpochSecond(OffsetDateTime.now().getOffset()))
                .build();
    }

    Function<String, Long> getParseSecondsFunction(String formatStr) {
        final String patternStr;
        if (beSupportFormatMap.containsValue(formatStr)) {
            patternStr = formatStr;
        } else if (beSupportFormatMap.containsKey(formatStr)) {
            patternStr = beSupportFormatMap.get(formatStr);
        } else {
            return null;
        }

        return parseMillisFunctionMap.get(patternStr);
    }

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (!(expr instanceof BinaryPredicate)) {
            return expr;
        }
        BinaryPredicate bp = (BinaryPredicate) expr;
        Expr left = bp.getChild(0);
        if (!(left instanceof FunctionCallExpr)) {
            return expr;
        }
        FunctionCallExpr fce = (FunctionCallExpr) left;
        if (!fce.getFnName().getFunction().equalsIgnoreCase("from_unixtime")) {
            return expr;
        }
        FunctionParams params = fce.getParams();
        if (params == null) {
            return expr;
        }
        // definition: from_unixtime(int, format)
        // TODO: from_unixtime(col, format) needs to be processed carefully, now only rewrite from_unixtime(col)
        //  to avoid wrong results.
        if (params.exprs().size() != 1) {
            return expr;
        }
        Expr paramSlot = params.exprs().get(0);
        if (!(paramSlot instanceof SlotRef)) {
            return expr;
        }
        SlotRef sr = (SlotRef) paramSlot;
        if (!sr.getColumn().getType().isBigIntType()) {
            return new BoolLiteral(false);
        }
        Expr right = bp.getChild(1);
        if (!(right instanceof LiteralExpr)) {
            return expr;
        }
        LiteralExpr le = (LiteralExpr) right;
        final String formatStr;
        // default format is "yyyy-MM-dd HH:mm:ss" (%Y-%m-%d %H:%i:%s)
        if (params.exprs().size() == 1) {
            formatStr = "yyyy-MM-dd HH:mm:ss";
        } else {
            LiteralExpr fm = (LiteralExpr) params.exprs().get(1);
            formatStr = fm.getStringValue();
        }
        Function<String, Long> parseSecondsFunction = getParseSecondsFunction(formatStr);
        if (null == parseSecondsFunction) {
            return expr;
        }

        try {
            Expr literalExpr = LiteralExpr.create(
                    String.valueOf(parseSecondsFunction.apply(le.getStringValue())), Type.BIGINT);
            // it must adds low bound 0, because when a field contains negative data like -100,
            // it will be queried as a result
            if (bp.getOp() == BinaryPredicate.Operator.LT || bp.getOp() == BinaryPredicate.Operator.LE) {
                BinaryPredicate r = new BinaryPredicate(bp.getOp(), sr, literalExpr);
                BinaryPredicate l = new BinaryPredicate(BinaryPredicate.Operator.GE, sr,
                        LiteralExpr.create("0", Type.BIGINT));
                return new CompoundPredicate(CompoundPredicate.Operator.AND, r, l);
            } else if (bp.getOp() == BinaryPredicate.Operator.GT || bp.getOp() == BinaryPredicate.Operator.GE) {
                // also it must adds upper bound 253402271999, because from_unixtime support time range is
                // [1970-01-01 00:00:00 ~ 9999-12-31 23:59:59]
                BinaryPredicate l = new BinaryPredicate(bp.getOp(), sr, literalExpr);
                BinaryPredicate r = new BinaryPredicate(BinaryPredicate.Operator.LE, sr,
                        LiteralExpr.create("253402271999", Type.BIGINT));
                return new CompoundPredicate(CompoundPredicate.Operator.AND, r, l);
            } else {
                return new BinaryPredicate(bp.getOp(), sr, literalExpr);
            }
        } catch (DateTimeParseException e) {
            return expr;
        }
    }
}
