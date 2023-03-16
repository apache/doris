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

/** java's enum declaration. */
public class EnumDeclaration extends TypeDeclaration {
    public final List<TypeType> implementTypes;
    public final List<EnumConstant> constants;

    /** constructor. */
    public EnumDeclaration(QualifiedName packageName, List<ImportDeclaration> imports,
            ClassOrInterfaceModifier modifier, String name, List<TypeType> implementTypes,
            List<EnumConstant> constants, List<TypeDeclaration> children) {
        super(packageName, imports, modifier, name, children);
        this.implementTypes = ImmutableList.copyOf(implementTypes);
        this.constants = ImmutableList.copyOf(constants);
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
        buffer.append(mod).append("enum ").append(name).append(" ");
        if (!implementTypes.isEmpty()) {
            buffer.append("implements ").append(Joiner.on(", ").join(implementTypes)).append(" ");
        }
        buffer.append("{\n");
        if (!constants.isEmpty()) {
            buffer.append("  ").append(Joiner.on(", ").join(constants)).append(";\n");
        }
        buffer.append("}\n");
        return buffer.toString();
    }
}
