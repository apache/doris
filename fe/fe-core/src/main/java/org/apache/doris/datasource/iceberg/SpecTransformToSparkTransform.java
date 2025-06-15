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

package org.apache.doris.datasource.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;

import java.util.Map;
import java.util.function.Function;

class SpecTransformToSparkTransform implements PartitionSpecVisitor<Transform> {
    private final Map<Integer, String> quotedNameById;

    SpecTransformToSparkTransform(Schema schema) {
        this.quotedNameById = indexQuotedNameById(schema);
    }

    @Override
    public Transform identity(String sourceName, int sourceId) {
        return Expressions.identity(quotedName(sourceId));
    }

    @Override
    public Transform bucket(String sourceName, int sourceId, int numBuckets) {
        return Expressions.bucket(numBuckets, quotedName(sourceId));
    }

    @Override
    public Transform truncate(String sourceName, int sourceId, int width) {
        NamedReference column = Expressions.column(quotedName(sourceId));
        return Expressions.apply("truncate", Expressions.literal(width), column);
    }

    @Override
    public Transform year(String sourceName, int sourceId) {
        return Expressions.years(quotedName(sourceId));
    }

    @Override
    public Transform month(String sourceName, int sourceId) {
        return Expressions.months(quotedName(sourceId));
    }

    @Override
    public Transform day(String sourceName, int sourceId) {
        return Expressions.days(quotedName(sourceId));
    }

    @Override
    public Transform hour(String sourceName, int sourceId) {
        return Expressions.hours(quotedName(sourceId));
    }

    @Override
    public Transform alwaysNull(int fieldId, String sourceName, int sourceId) {
        // do nothing for alwaysNull, it doesn't need to be converted to a transform
        return null;
    }

    @Override
    public Transform unknown(int fieldId, String sourceName, int sourceId, String transform) {
        return Expressions.apply(transform, Expressions.column(quotedName(sourceId)));
    }

    private String quotedName(int id) {
        return quotedNameById.get(id);
    }

    private Map<Integer, String> indexQuotedNameById(Schema schema) {
        Function<String, String> quotingFunc = name -> String.format("`%s`", name.replace("`", "``"));
        return TypeUtil.indexQuotedNameById(schema.asStruct(), quotingFunc);
    }

}
