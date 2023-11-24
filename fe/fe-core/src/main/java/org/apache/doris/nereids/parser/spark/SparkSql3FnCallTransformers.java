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

package org.apache.doris.nereids.parser.spark;

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
        doRegister("get_json_object", 2, "json_extract",
                Lists.newArrayList(
                        PlaceholderExpression.of(Expression.class, 1),
                        PlaceholderExpression.of(Expression.class, 2)), true);

        doRegister("get_json_object", 2, "json_extract",
                Lists.newArrayList(
                        PlaceholderExpression.of(Expression.class, 1),
                        PlaceholderExpression.of(Expression.class, 2)), false);

        doRegister("split", 2, "split_by_string",
                Lists.newArrayList(
                        PlaceholderExpression.of(Expression.class, 1),
                        PlaceholderExpression.of(Expression.class, 2)), true);
        doRegister("split", 2, "split_by_string",
                Lists.newArrayList(
                        PlaceholderExpression.of(Expression.class, 1),
                        PlaceholderExpression.of(Expression.class, 2)), false);
        // TODO: add other function transformer
    }

    @Override
    protected void registerComplexTransformers() {
        // TODO: add other complex function transformer
    }

    static class SingletonHolder {
        private static final SparkSql3FnCallTransformers INSTANCE = new SparkSql3FnCallTransformers();
    }

    public static SparkSql3FnCallTransformers getSingleton() {
        return SingletonHolder.INSTANCE;
    }
}
