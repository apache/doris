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

package org.apache.doris.plugin.dialect.spark;

import org.apache.doris.nereids.analyzer.PlaceholderExpression;
import org.apache.doris.nereids.parser.AbstractFnCallTransformer;
import org.apache.doris.nereids.parser.AbstractFnCallTransformers;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.Lists;

/**
 * The builder and factory for spark-sql 3.x {@link AbstractFnCallTransformer},
 * and supply transform facade ability.
 */
public class SparkSql3FnCallTransformers extends AbstractFnCallTransformers {

    private SparkSql3FnCallTransformers() {
    }

    @Override
    protected void registerTransformers() {
        // register json functions
        registerJsonFunctionTransformers();
        // register string functions
        registerStringFunctionTransformers();
        // register date functions
        registerDateFunctionTransformers();
        // register numeric functions
        registerNumericFunctionTransformers();
        // TODO: add other function transformer
    }

    @Override
    protected void registerComplexTransformers() {
        DateTruncFnCallTransformer dateTruncFnCallTransformer = new DateTruncFnCallTransformer();
        doRegister(dateTruncFnCallTransformer.getSourceFnName(), dateTruncFnCallTransformer);
        // TODO: add other complex function transformer
    }

    private void registerJsonFunctionTransformers() {
        doRegister("get_json_object", "json_extract",
                Lists.newArrayList(
                        PlaceholderExpression.of(Expression.class, 1),
                        PlaceholderExpression.of(Expression.class, 2)));
    }

    private void registerStringFunctionTransformers() {
        doRegister("split", "split_by_string",
                Lists.newArrayList(
                        PlaceholderExpression.of(Expression.class, 1),
                        PlaceholderExpression.of(Expression.class, 2)));
    }

    private void registerDateFunctionTransformers() {
        // spark-sql support to_date(date_str, fmt) function but doris only support to_date(date_str)
        // here try to compat with this situation by using str_to_date(str, fmt),
        // this function support the following three formats which can handle the mainly situations:
        //  1. yyyyMMdd
        //  2. yyyy-MM-dd
        //  3. yyyy-MM-dd HH:mm:ss
        doRegister("to_date", "str_to_date",
                Lists.newArrayList(
                        PlaceholderExpression.of(Expression.class, 1),
                        PlaceholderExpression.of(Expression.class, 2)));
    }

    private void registerNumericFunctionTransformers() {
        doRegister("mean", "avg",
                Lists.newArrayList(PlaceholderExpression.of(Expression.class, 1)));
    }

    static class SingletonHolder {
        private static final SparkSql3FnCallTransformers INSTANCE = new SparkSql3FnCallTransformers();
    }

    public static SparkSql3FnCallTransformers getSingleton() {
        return SingletonHolder.INSTANCE;
    }
}
