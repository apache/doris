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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Objects;

/**
 * represent function arguments
 */
public class FunctionArgTypesInfo {
    private final List<DataType> argTypeDefs;
    private final boolean isVariadic;

    // set after analyze
    private Type[] argTypes;

    public FunctionArgTypesInfo(List<DataType> argTypeDefs, boolean isVariadic) {
        this.argTypeDefs = Utils.fastToImmutableList(Objects.requireNonNull(argTypeDefs,
                "argTypeDefs should not be null"));
        this.isVariadic = isVariadic;
    }

    public Type[] getArgTypes() {
        return argTypes;
    }

    public List<DataType> getArgTypeDefs() {
        return argTypeDefs;
    }

    public boolean isVariadic() {
        return isVariadic;
    }

    /**
     * validate
     */
    public void analyze() {
        argTypes = new Type[argTypeDefs.size()];
        int i = 0;
        for (DataType dataType : argTypeDefs) {
            dataType.validateDataType();
            argTypes[i++] = dataType.toCatalogDataType();
        }
    }
}
