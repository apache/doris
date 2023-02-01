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
import org.apache.doris.thrift.TTypeNodeType;

public class VariantType extends Type {
    public VariantType() {

    }

    @Override
    public String toSql(int depth) {
        return "VARIANT";
    }

    @Override
    protected String prettyPrint(int lpad) {
        return "VARIANT";
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof VariantType;
    }

    @Override
    public void toThrift(TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        node.setType(TTypeNodeType.VARIANT);
    }

    @Override
    public String toString() {
        return toSql(0);
    }

    @Override
    public PrimitiveType getPrimitiveType() {
        return PrimitiveType.VARIANT;
    }

    @Override
    public boolean supportsTablePartitioning() {
        return false;
    }

    @Override
    public int getSlotSize() {
        return PrimitiveType.VARIANT.getSlotSize();
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public boolean matchesType(Type t) {
        return t.isVariantType() || t.isStringType();
    }
}
