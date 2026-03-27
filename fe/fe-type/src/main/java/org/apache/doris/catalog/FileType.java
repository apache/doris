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
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TTypeNodeType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

/**
 * FILE is a semantic first-class type backed by a fixed physical struct schema in BE.
 */
public class FileType extends Type {
    public static final FileType INSTANCE = new FileType();

    private FileType() {
    }

    @Override
    protected String toSql(int depth) {
        return "file";
    }

    @Override
    protected String prettyPrint(int lpad) {
        return "FILE";
    }

    @Override
    public PrimitiveType getPrimitiveType() {
        return PrimitiveType.FILE;
    }

    @Override
    public boolean matchesType(Type t) {
        return t instanceof FileType;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof FileType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(PrimitiveType.FILE);
    }

    @Override
    public void toThrift(TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        node.setType(TTypeNodeType.SCALAR);
        TScalarType scalarType = new TScalarType();
        scalarType.setType(PrimitiveType.FILE.toThrift());
        node.setScalarType(scalarType);
        container.types.add(node);
    }

    @Override
    public TColumnType toColumnTypeThrift() {
        TColumnType thrift = new TColumnType();
        thrift.type = PrimitiveType.FILE.toThrift();
        return thrift;
    }

    @Override
    public int getSlotSize() {
        return PrimitiveType.FILE.getSlotSize();
    }
}
