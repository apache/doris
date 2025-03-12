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
import org.apache.doris.thrift.TPrimitiveType;

public class JavaUdfMapType extends JavaUdfDataType {
    private Type keyType;
    private Type valueType;
    private int keyScale;
    private int valueScale;

    public JavaUdfMapType(String description, TPrimitiveType thriftType, int len) {
        super(description, thriftType, len);
        this.keyType = null;
        this.valueType = null;
        this.keyScale = 0;
        this.valueScale = 0;
    }

    public JavaUdfMapType(JavaUdfMapType mapType) {
        super(mapType);
        this.keyType = mapType.keyType;
        this.valueType = mapType.valueType;
        this.keyScale = mapType.keyScale;
        this.valueScale = mapType.valueScale;
    }

    public JavaUdfMapType(Type keyType, Type valueType) {
        super("MAP_TYPE", TPrimitiveType.MAP, 0);
        this.keyType = keyType;
        this.valueType = valueType;
        this.keyScale = 0;
        this.valueScale = 0;
    }

    @Override
    public String toString() {
        return super.toString() + " key: " + keyType.toString() + " sql: " + keyType.toString() + " value: "
                + valueType.toString() + " sql: " + valueType.toString();
    }

    public Type getKeyType() {
        return keyType;
    }

    public Type getValueType() {
        return valueType;
    }

    public void setKeyType(Type type) {
        this.keyType = type;
    }

    public void setValueType(Type type) {
        this.valueType = type;
    }

    public void setKeyScale(int scale) {
        this.keyScale = scale;
    }

    public void setValueScale(int scale) {
        this.valueScale = scale;
    }

    public int getKeyScale() {
        return keyScale;
    }

    public int getValueScale() {
        return valueScale;
    }
}
