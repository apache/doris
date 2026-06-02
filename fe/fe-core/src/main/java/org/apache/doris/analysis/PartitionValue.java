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

package org.apache.doris.analysis;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class PartitionValue {
    public static final PartitionValue MAX_VALUE = new PartitionValue();

    private LiteralExpr value;
    private String stringValue;
    private boolean isNullPartition = false;

    private PartitionValue() {
        this.value = MaxLiteral.MAX_VALUE;
        this.stringValue = "MAXVALUE";
    }

    public PartitionValue(LiteralExpr value) {
        this(value, false, value.getStringValue());
    }

    public PartitionValue(LiteralExpr value, boolean isNullPartition) {
        this(value, isNullPartition, value == null ? null : value.getStringValue());
    }

    public PartitionValue(LiteralExpr value, boolean isNullPartition, String stringValue) {
        if (isNullPartition) {
            Preconditions.checkArgument(value instanceof NullLiteral);
        }
        this.value = Preconditions.checkNotNull(value);
        this.isNullPartition = isNullPartition;
        this.stringValue = stringValue != null ? stringValue : this.value.getStringValue();
    }

    public LiteralExpr getValue() {
        return value;
    }

    public boolean isMax() {
        return this == MAX_VALUE;
    }

    public String getStringValue() {
        if (isMax()) {
            return "MAXVALUE";
        } else {
            return stringValue;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionValue that = (PartitionValue) o;
        boolean result = isNullPartition == that.isNullPartition
                && Objects.equal(value, that.value)
                && Objects.equal(stringValue, that.stringValue);
        return result;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value, stringValue, isNullPartition);
    }

    public boolean isNullPartition() {
        return isNullPartition;
    }
}
