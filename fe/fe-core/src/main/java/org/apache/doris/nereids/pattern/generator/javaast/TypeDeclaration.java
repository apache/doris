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

/** java's type declaration. */
public abstract class TypeDeclaration implements JavaAstNode {
    public final Optional<QualifiedName> packageName;
    public final List<ImportDeclaration> imports;
    public final ClassOrInterfaceModifier modifiers;
    public final String name;
    public final List<TypeDeclaration> children;

    /** type declaration's constructor. */
    public TypeDeclaration(QualifiedName packageName, List<ImportDeclaration> imports,
            ClassOrInterfaceModifier modifiers, String name, List<TypeDeclaration> children) {
        this.packageName = Optional.ofNullable(packageName);
        this.imports = ImmutableList.copyOf(imports);
        this.modifiers = modifiers;
        this.name = name;
        this.children = ImmutableList.copyOf(children);
    }

    public String getFullQualifiedName() {
        return getFullQualifiedName(packageName, name);
    }

    /** function to concat package name and type name. */
    public static String getFullQualifiedName(Optional<QualifiedName> packageName, String name) {
        return packageName.map(qualifiedName -> Joiner.on(".").join(qualifiedName.identifiers) + "." + name)
                .orElse(name);
    }
}
