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

package org.apache.doris.optimizer.base;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.operator.OptExpressionHandle;
import org.apache.doris.optimizer.operator.OptLogical;

public class RequiredLogicalProperty extends RequiredProperty {

    public RequiredLogicalProperty() {
        this.columns = new OptColumnRefSet();
    }

    public static RequiredLogicalProperty createEmptyProperty() {
        final RequiredLogicalProperty property = new RequiredLogicalProperty();
        property.columns = new OptColumnRefSet();
        return property;
    }

    @Override
    public void compute(OptExpressionHandle exprHandle, RequiredProperty parent, int childIndex) {
        Preconditions.checkArgument(parent instanceof RequiredLogicalProperty,
                "parent can only be logical property.");
        final OptLogical logical = (OptLogical) exprHandle.getOp();
        columns = logical.requiredStatForChild(exprHandle, (RequiredLogicalProperty)parent, childIndex);
    }

    public boolean isDifferent(RequiredLogicalProperty property) {
        return columns.isDifferent(property.columns);
    }

    public boolean isEmpty() {
        return columns.cardinality() == 0;
    }

    @Override
    public int hashCode() {
        return columns.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof RequiredLogicalProperty)) {
            return false;
        }

        if (obj == this) {
            return true;
        }
        final RequiredLogicalProperty other = (RequiredLogicalProperty) obj;
        return columns.equals(other.columns);
    }
}
