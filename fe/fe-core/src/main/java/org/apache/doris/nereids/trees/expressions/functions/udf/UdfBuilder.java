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

package org.apache.doris.nereids.trees.expressions.functions.udf;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.Udf;
import org.apache.doris.nereids.types.DataType;

import java.util.List;

/**
 * abstract class for udf function builder
 */
public abstract class UdfBuilder extends FunctionBuilder {
    public abstract List<DataType> getArgTypes();

    public abstract List<FunctionSignature> getSignatures();

    public boolean hasVarArguments() {
        return getSignatures().get(0).hasVarArgs;
    }

    /**
     * The catalog UDF instance this builder produces, or null when the builder does not carry one.
     *
     * The INSTANCE is the only place a Java / Python UDF's nullability lives (a
     * {@code Function.NullableMode} chosen at CREATE FUNCTION time), and SPM's dependency
     * fingerprint has to include it: re-creating the very same function with another nullability
     * changes what the optimizer may do with it while the class name and the signature stay
     * identical.
     *
     * @return the UDF instance, or null when this builder holds none
     */
    public Udf getUdf() {
        return null;
    }
}
