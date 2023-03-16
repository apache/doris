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

package org.apache.doris.nereids.pattern.generator.javaast;

import java.util.Optional;

/** java generic type argument. */
public class TypeArgument implements JavaAstNode {
    /** generic type. */
    public enum ArgType {
        NORMAL, EXTENDS, SUPER, UNKNOWN
    }

    private final ArgType argType;
    private final Optional<TypeType> typeType;

    public TypeArgument(ArgType argType, TypeType typeType) {
        this.argType = argType;
        this.typeType = Optional.ofNullable(typeType);
    }

    @Override
    public String toString() {
        switch (argType) {
            case NORMAL:
                return typeType.get().toString();
            case EXTENDS:
                return "? extends " + typeType.get();
            case SUPER:
                return "? super " + typeType.get();
            case UNKNOWN:
                return "?";
            default:
                throw new UnsupportedOperationException("Unknown argument type: " + argType);
        }
    }
}
