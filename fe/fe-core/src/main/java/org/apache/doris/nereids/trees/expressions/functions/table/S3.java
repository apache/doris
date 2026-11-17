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

package org.apache.doris.nereids.trees.expressions.functions.table;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.logical.SupportPruneNestedColumn;
import org.apache.doris.nereids.trees.plans.logical.SupportPruneNestedColumnFormats;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.tablefunction.S3TableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionIf;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** s3 */
public class S3 extends TableValuedFunction implements SupportPruneNestedColumn {
    private final List<TBrokerFileStatus> fileStatuses;

    public S3(Properties properties) {
        this(properties, null);
    }

    // Task-only inputs must not register another SQL function overload.
    private S3(Properties properties, List<TBrokerFileStatus> fileStatuses) {
        super("s3", properties);
        this.fileStatuses = fileStatuses == null ? null : ImmutableList.copyOf(fileStatuses);
    }

    public static S3 withFiles(Properties properties, List<TBrokerFileStatus> fileStatuses) {
        return new S3(properties, fileStatuses);
    }

    @Override
    protected boolean extraEquals(Expression that) {
        return super.extraEquals(that) && Objects.equals(fileStatuses, ((S3) that).fileStatuses);
    }

    @Override
    public int computeHashCode() {
        return Objects.hash(super.computeHashCode(), fileStatuses);
    }

    @Override
    public FunctionSignature customSignature() {
        return FunctionSignature.of(AnyDataType.INSTANCE_WITHOUT_INDEX, getArgumentsTypes());
    }

    @Override
    protected TableValuedFunctionIf toCatalogFunction() {
        try {
            Map<String, String> arguments = getTVFProperties().getMap();
            return new S3TableValuedFunction(arguments, fileStatuses);
        } catch (Throwable t) {
            throw new AnalysisException("Can not build s3(): " + t.getMessage(), t);
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitS3(this, context);
    }

    @Override
    public boolean supportPruneNestedColumn() {
        return SupportPruneNestedColumnFormats.supportFormat(getTVFProperties());
    }
}
