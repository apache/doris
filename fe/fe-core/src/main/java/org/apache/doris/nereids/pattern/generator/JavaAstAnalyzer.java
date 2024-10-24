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

package org.apache.doris.nereids.pattern.generator;

import org.apache.doris.nereids.pattern.generator.javaast.ClassDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.ClassOrInterfaceType;
import org.apache.doris.nereids.pattern.generator.javaast.EnumDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.IdentifyTypeArgumentsPair;
import org.apache.doris.nereids.pattern.generator.javaast.ImportDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.InterfaceDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.QualifiedName;
import org.apache.doris.nereids.pattern.generator.javaast.TypeDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.TypeType;

import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

/** JavaAstAnalyzer */
public class JavaAstAnalyzer {
    protected final Map<String, TypeDeclaration> name2Ast = new LinkedHashMap<>();
    protected final IdentityHashMap<TypeDeclaration, String> ast2Name = new IdentityHashMap<>();
    protected final IdentityHashMap<TypeDeclaration, Map<String, String>> ast2Import = new IdentityHashMap<>();
    protected final IdentityHashMap<TypeDeclaration, Set<String>> parentClassMap = new IdentityHashMap<>();
    protected final Map<String, Set<String>> parentNameMap = new LinkedHashMap<>();
    protected final Map<String, Set<String>> childrenNameMap = new LinkedHashMap<>();

    /** add java AST. */
    public void addAsts(List<TypeDeclaration> typeDeclarations) {
        for (TypeDeclaration typeDeclaration : typeDeclarations) {
            addAst(Optional.empty(), typeDeclaration);
        }
    }

    public IdentityHashMap<TypeDeclaration, Set<String>> getParentClassMap() {
        return parentClassMap;
    }

    public Map<String, Set<String>> getParentNameMap() {
        return parentNameMap;
    }

    public Map<String, Set<String>> getChildrenNameMap() {
        return childrenNameMap;
    }

    /** getType */
    public Optional<TypeDeclaration> getType(TypeDeclaration typeDeclaration, TypeType type) {
        String typeName = analyzeClass(new LinkedHashSet<>(), typeDeclaration, type);
        if (typeName != null) {
            TypeDeclaration ast = name2Ast.get(typeName);
            return Optional.ofNullable(ast);
        }

        return Optional.empty();
    }

    protected void analyze() {
        analyzeImport();
        analyzeParentClass();
        analyzeParentName();
        analyzeChildrenName();
    }

    private void analyzeImport() {
        for (TypeDeclaration typeDeclaration : name2Ast.values()) {
            Map<String, String> imports = new LinkedHashMap<>();
            for (ImportDeclaration importDeclaration : typeDeclaration.imports) {
                QualifiedName name = importDeclaration.name;
                if (!importDeclaration.isStatic && !importDeclaration.importAll
                        && name.suffix().isPresent()) {
                    String importName = Joiner.on(".").join(name.identifiers);
                    imports.put(name.suffix().get(), importName);
                }
            }
            ast2Import.put(typeDeclaration, imports);
        }
    }

    private void analyzeParentClass() {
        for (TypeDeclaration typeDeclaration : name2Ast.values()) {
            analyzeParentClass(new LinkedHashSet<>(), typeDeclaration);
        }
    }

    private void analyzeParentClass(Set<String> parentClasses, TypeDeclaration typeDeclaration) {
        Set<String> currentParentClasses = new LinkedHashSet<>();
        if (typeDeclaration instanceof InterfaceDeclaration) {
            for (TypeType extendsType : ((InterfaceDeclaration) typeDeclaration).extendsTypes) {
                analyzeClass(currentParentClasses, typeDeclaration, extendsType);
            }
        } else if (typeDeclaration instanceof EnumDeclaration) {
            for (TypeType implementType : ((EnumDeclaration) typeDeclaration).implementTypes) {
                analyzeClass(currentParentClasses, typeDeclaration, implementType);
            }
        } else if (typeDeclaration instanceof ClassDeclaration) {
            ClassDeclaration classDeclaration = (ClassDeclaration) typeDeclaration;
            classDeclaration.extendsType.ifPresent(
                    typeType -> analyzeClass(currentParentClasses, typeDeclaration, typeType));
            if (!classDeclaration.implementTypes.isEmpty()) {
                for (TypeType implementType : classDeclaration.implementTypes) {
                    analyzeClass(currentParentClasses, typeDeclaration, implementType);
                }
            }
        }
        parentClassMap.put(typeDeclaration, currentParentClasses);
        parentClasses.addAll(currentParentClasses);
    }

    private void analyzeParentName() {
        for (Entry<TypeDeclaration, Set<String>> entry : parentClassMap.entrySet()) {
            String parentName = entry.getKey().getFullQualifiedName();
            parentNameMap.put(parentName, entry.getValue());
        }
    }

    private void analyzeChildrenName() {
        for (Entry<String, TypeDeclaration> entry : name2Ast.entrySet()) {
            Set<String> parentNames = parentClassMap.get(entry.getValue());
            for (String parentName : parentNames) {
                Set<String> childrenNames = childrenNameMap.get(parentName);
                if (childrenNames == null) {
                    childrenNames = new LinkedHashSet<>();
                    childrenNameMap.put(parentName, childrenNames);
                }
                childrenNames.add(entry.getKey());
            }
        }
    }

    private String analyzeClass(Set<String> parentClasses, TypeDeclaration typeDeclaration, TypeType type) {
        if (type.classOrInterfaceType.isPresent()) {
            List<String> identifiers = new ArrayList<>();
            ClassOrInterfaceType classOrInterfaceType = type.classOrInterfaceType.get();
            for (IdentifyTypeArgumentsPair identifyTypeArgument : classOrInterfaceType.identifyTypeArguments) {
                identifiers.add(identifyTypeArgument.identifier);
            }
            String className = Joiner.on(".").join(identifiers);

            if (analyzeIfExist(parentClasses, className)) {
                parentClasses.add(className);
                return className;
            }
            Optional<String> importName = findFullImportName(typeDeclaration, className);
            if (importName.isPresent() && analyzeIfExist(parentClasses, importName.get())) {
                parentClasses.add(importName.get());
                return importName.get();
            }
            if (typeDeclaration.packageName.isPresent()) {
                String currentPackageClass = Joiner.on(".")
                        .join(typeDeclaration.packageName.get().identifiers) + "." + className;
                if (analyzeIfExist(parentClasses, currentPackageClass)) {
                    parentClasses.add(currentPackageClass);
                    return currentPackageClass;
                }
            }
            parentClasses.add(className);
            return className;
        } else if (type.primitiveType.isPresent()) {
            parentClasses.add(type.primitiveType.get());
            return type.primitiveType.get();
        } else {
            return null;
        }
    }

    private boolean analyzeIfExist(Set<String> parentClasses, String name) {
        if (name2Ast.get(name) != null) {
            analyzeParentClass(parentClasses, name2Ast.get(name));
            return true;
        } else {
            return false;
        }
    }

    private Optional<String> findFullImportName(TypeDeclaration typeDeclaration, String name) {
        Map<String, String> name2FullName = ast2Import.get(typeDeclaration);
        if (name2FullName != null && name2FullName.get(name) != null) {
            return Optional.of(name2FullName.get(name));
        }
        return Optional.empty();
    }

    private void addAst(Optional<String> outerClassName, TypeDeclaration typeDeclaration) {
        String nameWithOuterClass;
        if (!outerClassName.isPresent()) {
            nameWithOuterClass = typeDeclaration.name;
            String fullQualifiedName = typeDeclaration.getFullQualifiedName();
            name2Ast.put(fullQualifiedName, typeDeclaration);
            ast2Name.put(typeDeclaration, fullQualifiedName);
        } else if (typeDeclaration.packageName.isPresent()) {
            nameWithOuterClass = outerClassName.get() + "." + typeDeclaration.name;
            String fullName = typeDeclaration.packageName.get() + "." + nameWithOuterClass;
            name2Ast.put(fullName, typeDeclaration);
            ast2Name.put(typeDeclaration, fullName);
        } else {
            nameWithOuterClass = outerClassName.get() + "." + typeDeclaration.name;
            name2Ast.put(nameWithOuterClass, typeDeclaration);
            ast2Name.put(typeDeclaration, nameWithOuterClass);
        }

        for (TypeDeclaration child : typeDeclaration.children) {
            addAst(Optional.of(nameWithOuterClass), child);
        }
    }
}
