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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/** java's interface declaration. */
public class InterfaceDeclaration extends TypeDeclaration {
    public final Optional<TypeParameters> typeParameters;
    public final List<TypeType> extendsTypes;

    /** constructor. */
    public InterfaceDeclaration(QualifiedName packageName, List<ImportDeclaration> imports,
            ClassOrInterfaceModifier modifier, String name, TypeParameters typeParameters,
            List<TypeType> extendsTypes, List<TypeDeclaration> children) {
        super(packageName, imports, modifier, name, children);
        this.typeParameters = Optional.ofNullable(typeParameters);
        this.extendsTypes = ImmutableList.copyOf(extendsTypes);
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        packageName.ifPresent(qualifiedName -> buffer.append("package ").append(qualifiedName).append(";\n\n"));

        if (!imports.isEmpty()) {
            for (ImportDeclaration importDeclaration : imports) {
                buffer.append(importDeclaration).append("\n");
            }
            buffer.append("\n");
        }
        String mod = modifiers.toString();
        if (!mod.isEmpty()) {
            mod += " ";
        }
        buffer.append(mod).append("interface ").append(name);
        typeParameters.ifPresent(buffer::append);
        buffer.append(" ");
        if (!extendsTypes.isEmpty()) {
            buffer.append("extends ").append(Joiner.on(", ").join(extendsTypes)).append(" ");
        }
        buffer.append("{}");
        return buffer.toString();
    }
}
