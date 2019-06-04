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
import org.apache.doris.optimizer.Optimizer;
import org.apache.doris.optimizer.base.OptProperty;
import org.apache.doris.optimizer.base.OptimizationContext;

// Base class for operation. Operation can be logical or physical
// Pattern is a special kind of operation which is only used in rules to
// match pattern and then do transform
// All operators have type property and some of them have other arguments
// for example, LogicalScan have property indicating which table is going
// to be scanned
public abstract class OptOperator {
    protected OptOperatorType type;

    protected OptOperator(OptOperatorType type) {
        this.type = type;
    }

    public OptOperatorType getType() { return type; }
    public String getName() { return type.getName(); }

    public boolean isLogical() { return false; }
    public boolean isPhysical() { return false; }
    public boolean isItem() { return false; }
    public boolean isLeaf() { return false; }
    // if this operator is pattern
    public boolean isPattern() { return false; }
    // If this operator is pattern and is leaf.
    public boolean isPatternAndLeaf() { return false; }
    public boolean isSubquery() { return false; }
    // If this operator is pattern and is tree.
    public boolean isPatternAndTree() { return false; }
    // If this operator is pattern and is multi tree.
    public boolean isPatternAndMultiTree() { return false; }
    public boolean isValidOptimizationContext(OptimizationContext context) { return true; }
    // If this operator care about its inputs' order. For join operator (A join B) is
    // not equal with (B join A), so it is order sensitive. And for union operator,
    // (A union B) is equal with (B union A), so it's order insensitive.
    public boolean isInputOrderSensitive() { return true; }

    public String debugString() {
        return type.getName();
    }

    // If operator need to print information in multilines,
    // except first line, new lines should be printed after prefix
    public String getExplainString(String prefix) {
        return type.getName();
    }

    // Create property for this operator. A item operator would create OptItemProperty
    // and a logical operator would create OptLogicalProperty
    public OptProperty createProperty() {
        Preconditions.checkArgument(false, "this should not be called, op=" + this);
        return null;
    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    // equals is used to find if there is also a redundant MultiExpression
    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return type == ((OptOperator) obj).type;
    }

    @Override
    public String toString() { return debugString(); }

    public enum AggType {
        GB_LOCAL,
        GB_GLOBAL,
        GB_INTERMEDIATE
    }

    public enum AggStage {
        TWO_STAGE_SCALAR_DQA,
        THREE_STAGE_SCALAR_DQA,
        OTHERS
    }
}
