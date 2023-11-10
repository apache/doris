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

import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TTypeDesc;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Map;

/**
 * Describes a TemplateType type, used for SQL function argument and return type,
 *  NOT used for table column type.
 */
public class TemplateType extends Type {

    @SerializedName(value = "name")
    private final String name;

    @SerializedName(value = "isVariadic")
    private final boolean isVariadic;

    public TemplateType(String name, boolean isVariadic) {
        this.name = name;
        this.isVariadic = isVariadic;
    }

    public TemplateType(String name) {
        this(name, false);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
        return PrimitiveType.TEMPLATE;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TemplateType)) {
            return false;
        }
        TemplateType o = (TemplateType) other;
        return o.name.equals(name) && o.isVariadic == isVariadic;
    }

    @Override
    public boolean matchesType(Type t) {
        // not matches any type
        return false;
    }

    @Override
    public boolean hasTemplateType() {
        return true;
    }

    @Override
    public boolean needExpandTemplateType() {
        return isVariadic;
    }

    @Override
    public Type specializeTemplateType(Type specificType, Map<String, Type> specializedTypeMap,
                                       boolean useSpecializedType, boolean enableDecimal256) throws TypeException {
        if (specificType.hasTemplateType() && !specificType.isNull()) {
            throw new TypeException(specificType + " should not hasTemplateType");
        }

        Type specializedType = specializedTypeMap.get(name);
        if (useSpecializedType) {
            if (specializedType == null) {
                throw new TypeException("template type " + name + " is not specialized yet");
            }
            return specializedType;
        }

        if (specializedType != null
                && !specificType.equals(specializedType)
                && !specificType.matchesType(specializedType)
                && !Type.isImplicitlyCastable(specificType, specializedType, true, enableDecimal256)
                && !Type.canCastTo(specificType, specializedType)) {
            throw new TypeException(
                String.format("can not specialize template type %s to %s since it's already specialized as %s",
                    name, specificType, specializedType));
        }

        if (specializedType == null) {
            specializedTypeMap.put(name, specificType);
        }
        return specializedTypeMap.get(name);
    }

    @Override
    public void collectTemplateExpandSize(Type[] args, Map<String, Integer> expandSizeMap)
            throws TypeException {
        Preconditions.checkState(isVariadic);
        expandSizeMap.computeIfAbsent(name, k -> args.length);
        if (expandSizeMap.get(name) != args.length) {
            throw new TypeException(
                String.format("can not expand variadic template type %s to %s size since it's "
                    + "already expand as %s size", name, args.length, expandSizeMap.get(name)));
        }
    }

    @Override
    public List<Type> expandVariadicTemplateType(Map<String, Integer> expandSizeMap) {
        if (needExpandTemplateType() && expandSizeMap.containsKey(name)) {
            List<Type> types = Lists.newArrayList();
            int size = expandSizeMap.get(name);
            for (int index = 0; index < size; index++) {
                types.add(new TemplateType(name + "_" + index));
            }
            return types;
        }
        return Lists.newArrayList(this);
    }

    @Override
    public String toSql(int depth) {
        return name;
    }

    @Override
    public String toString() {
        return toSql(0).toUpperCase();
    }

    @Override
    protected String prettyPrint(int lpad) {
        String leftPadding = Strings.repeat(" ", lpad);
        return leftPadding + toSql();
    }

    @Override
    public boolean supportSubType(Type subType) {
        throw new RuntimeException("supportSubType not implementd for TemplateType");
    }

    @Override
    public void toThrift(TTypeDesc container) {
        throw new RuntimeException("can not call toThrift on TemplateType");
    }

    @Override
    public TColumnType toColumnTypeThrift() {
        throw new RuntimeException("can not call toColumnTypeThrift on TemplateType");
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
