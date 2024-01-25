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

package org.apache.doris.catalog;

import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

public class AggStateType extends ScalarType {

    @SerializedName(value = "subTypes")
    private List<Type> subTypes;

    @SerializedName(value = "subTypeNullables")
    private List<Boolean> subTypeNullables;

    @SerializedName(value = "resultIsNullable")
    private Boolean resultIsNullable;

    @SerializedName(value = "functionName")
    private String functionName;

    public AggStateType(String functionName, Boolean resultIsNullable, List<Type> subTypes,
            List<Boolean> subTypeNullables) {
        super(PrimitiveType.AGG_STATE);
        Preconditions.checkState((subTypes == null) == (subTypeNullables == null));
        if (subTypes != null && subTypeNullables != null) {
            Preconditions.checkState(subTypes.size() == subTypeNullables.size(),
                    "AggStateType' subTypes.size()!=subTypeNullables.size()");
        }
        this.functionName = functionName;
        this.subTypes = subTypes;
        this.subTypeNullables = subTypeNullables;
        this.resultIsNullable = resultIsNullable;
    }

    public List<Type> getSubTypes() {
        return subTypes;
    }

    public List<Boolean> getSubTypeNullables() {
        return subTypeNullables;
    }

    public String getFunctionName() {
        return functionName;
    }

    public boolean getResultIsNullable() {
        return resultIsNullable;
    }

    @Override
    public String toSql(int depth) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("AGG_STATE(");
        for (int i = 0; i < subTypes.size(); i++) {
            if (i > 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append(subTypes.get(i).toSql());
            if (subTypeNullables.get(i)) {
                stringBuilder.append(" NULL");
            }
        }
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    @Override
    public void toThrift(TTypeDesc container) {
        super.toThrift(container);
        if (subTypes != null) {
            List<TTypeDesc> types = new ArrayList<TTypeDesc>();
            for (int i = 0; i < subTypes.size(); i++) {
                TTypeDesc desc = new TTypeDesc();
                desc.setTypes(new ArrayList<TTypeNode>());
                subTypes.get(i).toThrift(desc);
                desc.setIsNullable(subTypeNullables.get(i));
                types.add(desc);
            }
            container.setSubTypes(types);
        }
        container.setResultIsNullable(resultIsNullable);
        container.setFunctionName(functionName);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AggStateType)) {
            return false;
        }
        AggStateType other = (AggStateType) o;
        if ((subTypes == null) != (other.getSubTypes() == null)) {
            return false;
        }
        if (subTypes == null) {
            return true;
        }
        int subTypeNumber = subTypeNullables.size();
        if (subTypeNumber != other.subTypeNullables.size()) {
            return false;
        }
        for (int i = 0; i < subTypeNumber; i++) {
            if (!subTypeNullables.get(i).equals(other.subTypeNullables.get(i))) {
                return false;
            }
            if (!subTypes.get(i).equals(other.subTypes.get(i))) {
                return false;
            }
        }
        return true;
    }
}
