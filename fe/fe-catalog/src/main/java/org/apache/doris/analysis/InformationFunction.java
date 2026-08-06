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

import org.apache.doris.catalog.Type;

import com.google.gson.annotations.SerializedName;

public class InformationFunction extends Expr {
    @SerializedName("ft")
    private String funcType;
    private long intValue;
    private String strValue;

    private InformationFunction() {
        // only for serde
    }

    // First child is the comparison expr which should be in [lowerBound, upperBound].
    public InformationFunction(String funcType) {
        this.funcType = funcType;
    }

    protected InformationFunction(InformationFunction other) {
        super(other);
        funcType = other.funcType;
        intValue = other.intValue;
        strValue = other.strValue;
    }

    public String getStrValue() {
        return strValue;
    }

    public String getIntValue() {
        return String.valueOf(intValue);
    }

    public String getFuncType() {
        return funcType;
    }

    @Override
    public Expr clone() {
        return new InformationFunction(this);
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitInformationFunction(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof InformationFunction)) {
            return false;
        }

        if (!funcType.equals(((InformationFunction) obj).getFuncType())) {
            return false;
        }

        if (type.equals(Type.VARCHAR)) {
            if (!strValue.equals(((InformationFunction) obj).getStrValue())) {
                return false;
            }
        } else if (type.equals(Type.BIGINT)) {
            if (intValue != Integer.parseInt(((InformationFunction) obj).getIntValue())) {
                return false;
            }
        }
        return true;
    }
}
