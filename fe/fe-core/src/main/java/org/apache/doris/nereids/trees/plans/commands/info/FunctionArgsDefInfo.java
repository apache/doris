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
import org.apache.doris.nereids.util.TypeCoercionUtils;

import java.util.List;

/**
 * represent function arguments
 */
public class FunctionArgsDefInfo {
    private final List<DataType> argTypeDefs;
    private final boolean isVariadic;

    // set after analyze
    private Type[] argTypes;

    public FunctionArgsDefInfo(List<DataType> argTypeDefs, boolean isVariadic) {
        this.argTypeDefs = argTypeDefs;
        this.isVariadic = isVariadic;
    }

    public Type[] getArgTypes() {
        return argTypes;
    }

    public boolean isVariadic() {
        return isVariadic;
    }

    /**
     * validate
     */
    public void validate() {
        argTypes = new Type[argTypeDefs.size()];
        int i = 0;
        for (DataType dataType : argTypeDefs) {
            TypeCoercionUtils.validateDataType(dataType);
            argTypes[i++] = dataType.toCatalogDataType();
        }
    }
}
