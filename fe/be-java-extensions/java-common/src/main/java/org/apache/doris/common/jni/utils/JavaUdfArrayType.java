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

package org.apache.doris.common.jni.utils;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.exception.InternalException;
import org.apache.doris.thrift.TPrimitiveType;

public class JavaUdfArrayType extends JavaUdfDataType {
    private Type itemType;

    public JavaUdfArrayType(String description, TPrimitiveType thriftType, int len) {
        super(description, thriftType, len);
        this.itemType = null;
    }

    public JavaUdfArrayType(JavaUdfArrayType other) {
        super(other);
        this.itemType = other.itemType;
    }

    public JavaUdfArrayType(Type itemType) {
        super("ARRAY_TYPE", TPrimitiveType.ARRAY, 0);
        this.itemType = itemType;
    }

    @Override
    public String toString() {
        return super.toString() + " item: " + itemType.toString() + " sql: " + itemType.toString();
    }

    public Type getItemType() {
        return itemType;
    }

    public void setItemType(Type type) throws InternalException {
        if (this.itemType == null) {
            this.itemType = type;
        } else {
            if (!this.itemType.matchesType(type)) {
                throw new InternalException("udf type not matches origin type :" + this.itemType.toSql()
                        + " set type :" + type.toSql());
            }
        }
    }

}
