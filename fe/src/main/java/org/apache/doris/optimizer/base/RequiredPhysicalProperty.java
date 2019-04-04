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

import org.apache.doris.optimizer.OptUtils;
import org.apache.doris.optimizer.operator.OptExpressionHandle;
import org.apache.doris.optimizer.operator.OptPhysical;

public class RequiredPhysicalProperty {
    private OptColumnRefSet reqdColumns;
    private EnforceOrderProperty reqdOrder;

    public RequiredPhysicalProperty(
            OptColumnRefSet reqdColumns,
            EnforceOrderProperty reqdOrder) {
        this.reqdColumns = reqdColumns;
        this.reqdOrder = reqdOrder;
    }

    public OptColumnRefSet getReqdColumns() { return reqdColumns; }
    public EnforceOrderProperty getReqdOrder() { return reqdOrder; }

    // compute given child's required physical property, this can push down required property
    // such as we can push reqdOrder by through a limit node
    public void compute(OptExpressionHandle handle,
                        RequiredPhysicalProperty reqdProp,
                        int childIndex) {
        OptPhysical phy = null;
        reqdOrder = phy.getChildReqdOrder(handle, reqdProp.getReqdOrder(), childIndex);
    }

    @Override
    public int hashCode() {
        int code = reqdColumns.hashCode();
        return OptUtils.combineHash(code, reqdOrder.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RequiredPhysicalProperty)) {
            return false;
        }
        if (this == obj) {
            return false;
        }
        RequiredPhysicalProperty rhs = (RequiredPhysicalProperty) obj;
        return reqdColumns.equals(rhs.reqdColumns) && reqdOrder.equals(rhs.reqdOrder);
    }
}
