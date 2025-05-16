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

import lombok.Getter;

import java.util.List;

public class EncodingTree {
    @Getter
    private final int encodingNumber;
    private final List<EncodingTree> children;

    public EncodingTree(int encodingNumber, List<EncodingTree> children) {
        this.encodingNumber = encodingNumber;
        this.children = children;
    }

    public EncodingTree child(int index) {
        if (children == null) {
            throw new IllegalArgumentException("no children");
        }
        EncodingTree child = children.get(index);
        if (child == null) {
            throw new IllegalArgumentException("no child " + index);
        }
        return child;
    }

    public static String toSql(EncodingTree encodingTree) {
        if (null == encodingTree) {
            return "";
        }
        String encodingStr = EncodingInfo.getEncodingString(encodingTree.getEncodingNumber());
        if (encodingStr == null) {
            throw new IllegalArgumentException("unknown encoding number: " + encodingTree.getEncodingNumber());
        }
        if (EncodingInfo.isDefaultEncoding(encodingStr)) {
            return "";
        }
        return " ENCODING \"" + encodingStr + "\"";
    }
}
