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

import org.apache.doris.common.Config;
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TTypeNodeType;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AggStateType extends Type {

    @SerializedName(value = "subTypes")
    private List<Type> subTypes;

    @SerializedName(value = "subTypeNullables")
    private List<Boolean> subTypeNullables;

    @SerializedName(value = "resultIsNullable")
    private Boolean resultIsNullable;

    @SerializedName(value = "functionName")
    private String functionName;

    @SerializedName(value = "beExecVersion")
    private Integer beExecVersion;

    public AggStateType(String functionName, Boolean resultIsNullable, List<Type> subTypes,
            List<Boolean> subTypeNullables) {
        this(functionName, resultIsNullable, subTypes, subTypeNullables, Config.be_exec_version);
    }

    public AggStateType(String functionName, Boolean resultIsNullable, List<Type> subTypes,
            List<Boolean> subTypeNullables, int beExecVersion) {
        Objects.requireNonNull(subTypes, "subTypes should not be null");
        Objects.requireNonNull(subTypeNullables, "subTypeNullables should not be null");
        if (subTypes.size() != subTypeNullables.size()) {
            throw new IllegalStateException("AggStateType's subTypes.size() [" + subTypes.size()
                    + "] != subTypeNullables.size() [" + subTypeNullables.size() + "]");
        }
        this.functionName = functionName;
        this.subTypes = subTypes;
        this.subTypeNullables = subTypeNullables;
        this.resultIsNullable = resultIsNullable;
        this.beExecVersion = beExecVersion;
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

    public int getBeExecVersion() {
        if (beExecVersion == null) {
            // Old catalog JSON has no codec version. Keep its former version selection
            // below 20. For example, query version 16 must still select old function codecs.
            // An upgrade to 20 must not enable new bitmap markers for these old types.
            return Math.min(Config.be_exec_version, Config.UNIFIED_NULLABLE_AGGREGATION_MIN_BE_EXEC_VERSION - 1);
        }
        // New types retain their creation version through replay and query planning.
        // For example, query version 19 must not replace a stored type's version 20.
        return beExecVersion;
    }

    @Override
    public String toSql(int depth) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("agg_state<").append(functionName).append("(");
        for (int i = 0; i < subTypes.size(); i++) {
            if (i > 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append(subTypes.get(i).toSql());
            if (subTypeNullables.get(i)) {
                stringBuilder.append(" null");
            } else {
                stringBuilder.append(" not null");
            }
        }
        stringBuilder.append(")");
        stringBuilder.append(">");
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    protected String prettyPrint(int lpad) {
        return Strings.repeat(" ", lpad) + toSql();
    }

    @Override
    public void toThrift(TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        // ATTN: use scalar only for compatibility.
        node.setType(TTypeNodeType.SCALAR);
        TScalarType scalarType = new TScalarType();
        scalarType.setType(getPrimitiveType().toThrift());
        node.setScalarType(scalarType);
        List<TTypeDesc> types = new ArrayList<>();
        for (int i = 0; i < subTypes.size(); i++) {
            TTypeDesc desc = new TTypeDesc();
            desc.setTypes(new ArrayList<>());
            subTypes.get(i).toThrift(desc);
            desc.setIsNullable(subTypeNullables.get(i));
            types.add(desc);
        }
        container.setSubTypes(types);
        container.setResultIsNullable(resultIsNullable);
        container.setFunctionName(functionName);
        container.setBeExecVersion(getBeExecVersion());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AggStateType)) {
            return false;
        }
        AggStateType other = (AggStateType) o;
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

    public PrimitiveType getPrimitiveType() {
        return PrimitiveType.AGG_STATE;
    }

    @Override
    public int getSlotSize() {
        return PrimitiveType.AGG_STATE.getSlotSize();
    }

    @Override
    public boolean matchesType(Type t) {
        return t.isAggStateType() || t.isStringType();
    }
}
