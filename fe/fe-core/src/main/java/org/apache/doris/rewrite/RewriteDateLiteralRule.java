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
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.common.AnalysisException;

/**
 * this rule try to convert date expression, if date is invalid, it will be
 * converted into null literal to avoid scanning all partitions
 * if a date data is invalid, Doris will try to cast it as datetime firstly,
 * only support rewriting pattern: slot + operator + date literal
 * Examples:
 * date = "2020-10-32" will throw analysis exception when in on clause or where clause,
 * and be converted to be NULL when in other clause
 */
public class RewriteDateLiteralRule implements ExprRewriteRule {
    public final static ExprRewriteRule INSTANCE = new RewriteDateLiteralRule();
    private final static int ALLOW_SPACE_MASK = 4 | 64;
    private final static int MAX_DATE_PARTS = 8;
    private final static int YY_PART_YEAR = 70;
    private final static int[] DAYS_IN_MONTH = new int[] {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (!(expr instanceof BinaryPredicate)) return expr;
        Expr lchild = expr.getChild(0);
        if (!lchild.getType().isDateType()) {
            return expr;
        }
        Expr valueExpr = expr.getChild(1);
        if (!valueExpr.getType().isDateType()) {
            return expr;
        }
        if (!valueExpr.isConstant()) {
            return expr;
        }
        // Only consider CastExpr and try our best to convert non-date_literal to date_literal，to be compatible with MySQL
        if (valueExpr instanceof CastExpr) {
            Expr childExpr = valueExpr.getChild(0);
            if (childExpr instanceof LiteralExpr) {
                try {
                    String dateStr = childExpr.getStringValue();
                    expr.setChild(1, getDateLiteralfromDateStr(dateStr));
                } catch (AnalysisException e) {
                    if (clauseType == ExprRewriter.ClauseType.OTHER_CLAUSE) {
                        return new NullLiteral();
                    } else {
                        throw new AnalysisException("Incorrect datetime value: " + valueExpr.toSql() + " in expression: " + expr.toSql());
                    }
                }
            }
        }
        return expr;
    }

    // The interval format is that with no delimiters
    // YYYY-MM-DD HH-MM-DD.FFFFFF AM in default format, and now doris will skip part 6, 7
    // 0    1  2  3  4  5  6      7
    private DateLiteral getDateLiteralfromDateStr(String dateStr) throws AnalysisException {
        dateStr = dateStr.trim();
        if (dateStr.isEmpty()) {
            throw new AnalysisException("parse datetime value failed: " + dateStr);
        }
        int[] dateVal = new int[MAX_DATE_PARTS];
        int[] dateLen = new int[MAX_DATE_PARTS];

        // Fix year length
        int pre = 0;
        int pos = 0;
        while (pos < dateStr.length() && (Character.isDigit(dateStr.charAt(pos)) || dateStr.charAt(pos) == 'T')) {
            pos++;
        }
        int yearLen = 4;
        int digits = pos - pre;
        boolean isIntervalFormat = false;
        // For YYYYMMDD/YYYYMMDDHHMMSS is 4 digits years
        if (pos == dateStr.length() || dateStr.charAt(pos) == '.') {
            if (digits == 4 || digits == 8 || digits >= 14) {
                yearLen = 4;
            } else {
                yearLen = 2;
            }
            isIntervalFormat = true;
        }
        int fieldIdx = 0;
        int fieldLen = yearLen;
        while (pre < dateStr.length() && Character.isDigit(dateStr.charAt(pre)) && fieldIdx < MAX_DATE_PARTS - 1) {
            int start = pre;
            int temp_val = 0;
            boolean scanToDelim = (!isIntervalFormat) && (fieldIdx != 6);
            while (pre < dateStr.length() && Character.isDigit(dateStr.charAt(pre)) && (scanToDelim || fieldLen-- != 0)) {
                temp_val = temp_val * 10 + (dateStr.charAt(pre++) - '0');
            }
            dateVal[fieldIdx] = temp_val;
            dateLen[fieldIdx] = pre - start;
            fieldLen = 2;

            if (pre == dateStr.length()) {
                fieldIdx++;
                break;
            }

            if (fieldIdx == 2 && dateStr.charAt(pre) == 'T') {
                // YYYYMMDDTHHMMDD, skip 'T' and continue
                pre++;
                fieldIdx++;
                continue;
            }

            // Second part
            if (fieldIdx == 5) {
                if (dateStr.charAt(pre) == '.') {
                    pre++;
                    fieldLen = 6;
                } else if (Character.isDigit(dateStr.charAt(pre))) {
                    fieldIdx++;
                    break;
                }
                fieldIdx++;
                continue;
            }
            // escape separator
            while (pre < dateStr.length() && (Character.toString(dateStr.charAt(pre)).matches("\\p{Punct}"))
                    || Character.isSpaceChar(dateStr.charAt(pre))) {
                if (Character.isSpaceChar(dateStr.charAt(pre))) {
                    if (((1 << fieldIdx) & ALLOW_SPACE_MASK) == 0) {
                        throw new AnalysisException("parse datetime value failed: " + dateStr);
                    }
                }
                pre++;
            }
            fieldIdx++;
        }
        int numField = fieldIdx;
        if (!isIntervalFormat) {
            yearLen = dateLen[0];
        }
        for (; fieldIdx < MAX_DATE_PARTS; ++fieldIdx) {
            dateLen[fieldIdx] = 0;
            dateVal[fieldIdx] = 0;
        }
        if (yearLen == 2) {
            if (dateVal[0] < YY_PART_YEAR) {
                dateVal[0] += 2000;
            } else {
                dateVal[0] += 1900;
            }
        }

        if (numField < 3) {
            throw new AnalysisException("parse datetime value failed: " + dateStr);
        }
        return getValidDataLiteral(dateVal[0], dateVal[1], dateVal[2], dateVal[3], dateVal[4], dateVal[5]);
    }

    private boolean isLeapYear(int year) {
        return ((year % 4) == 0) && ((year % 100 != 0) || ((year % 400) == 0));
    }

    private Boolean CheckDateOutOfRange(int year, int month, int day) {
        if (month != 0 && month <= 12 && day > DAYS_IN_MONTH[month]) {
            // Feb 29 in leap year is valid.
            if (!(month == 2 && day == 29 && isLeapYear(year))) return true;
        }
        return year > 9999 || month > 12 || day > 31;
    }

    private DateLiteral getValidDataLiteral(int year, int month, int day, int hour, int minute, int second) throws AnalysisException {
        boolean timeOutOfRange = hour > 23 || minute > 59 || second > 59;
        if (timeOutOfRange || CheckDateOutOfRange(year, month, day)) {
            throw new AnalysisException("Datetime value is out of range: " +
                    String.format("%s-%s-%s %s:%s:%s", year, month, day, hour, minute, second));
        }
        return new DateLiteral(year, month, day, hour, minute, second);
    }
}
