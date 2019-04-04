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

package org.apache.doris.optimizer.operator;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.base.EnforceOrderProperty;
import org.apache.doris.optimizer.base.EnforceProperty;
import org.apache.doris.optimizer.base.OptOrderSpec;

public class OptPhysicalOlapScan extends OptPhysical {

    public OptPhysicalOlapScan() {
        super(OptOperatorType.OP_PHYSICAL_OLAP_SCAN);
    }

    //------------------------------------------------------------------------
    // Used to compute required property for children
    //------------------------------------------------------------------------
    @Override
    public EnforceOrderProperty getChildReqdOrder(
            OptExpressionHandle handle, EnforceOrderProperty reqdOrder, int childIndex) {
        Preconditions.checkArgument(false);
        return null;
    }

    //------------------------------------------------------------------------
    // Used to get operator's derived property
    //------------------------------------------------------------------------
    @Override
    public OptOrderSpec getOrderSpec(OptExpressionHandle exprHandle) {
        // now
        return OptOrderSpec.createEmpty();
    }

    //------------------------------------------------------------------------
    // Used to get enforce type for this operator
    //------------------------------------------------------------------------
    @Override
    public EnforceProperty.EnforceType getOrderEnforceType(OptExpressionHandle exprHandle,
                                                           EnforceOrderProperty enforceOrder) {
        return EnforceProperty.EnforceType.REQUIRED;
    }
}
