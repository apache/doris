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

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * used to analyze plan class extends hierarchy and then generated pattern builder methods.
 */
public class PatternGeneratorAnalyzer {
    private final Map<String, TypeDeclaration> name2Ast = new LinkedHashMap<>();
    private final IdentityHashMap<TypeDeclaration, String> ast2Name = new IdentityHashMap<>();
    private final IdentityHashMap<TypeDeclaration, Map<String, String>> ast2Import = new IdentityHashMap<>();
    private final IdentityHashMap<TypeDeclaration, Set<String>> parentClassMap = new IdentityHashMap<>();

    /** add java AST. */
    public void addAsts(List<TypeDeclaration> typeDeclarations) {
        for (TypeDeclaration typeDeclaration : typeDeclarations) {
            addAst(Optional.empty(), typeDeclaration);
        }
    }

    /** generate pattern methods. */
    public String generatePatterns() {
        analyzeImport();
        analyzeParentClass();
        return doGenerate();
    }

    Optional<TypeDeclaration> getType(TypeDeclaration typeDeclaration, TypeType type) {
        String typeName = analyzeClass(new LinkedHashSet<>(), typeDeclaration, type);
        if (typeName != null) {
            TypeDeclaration ast = name2Ast.get(typeName);
            return Optional.ofNullable(ast);
        }

        return Optional.empty();
    }

    private String doGenerate() {
        Map<ClassDeclaration, Set<String>> planClassMap = parentClassMap.entrySet().stream()
                .filter(kv -> kv.getValue().contains("org.apache.doris.nereids.trees.plans.Plan"))
                .filter(kv -> !kv.getKey().name.equals("GroupPlan"))
                .filter(kv -> !Modifier.isAbstract(kv.getKey().modifiers.mod)
                        && kv.getKey() instanceof ClassDeclaration)
                .collect(Collectors.toMap(kv -> (ClassDeclaration) kv.getKey(), kv -> kv.getValue()));

        List<PatternGenerator> generators = planClassMap.entrySet()
                .stream()
                .map(kv -> PatternGenerator.create(this, kv.getKey(), kv.getValue()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .sorted((g1, g2) -> {
                    // logical first
                    if (g1.isLogical() != g2.isLogical()) {
                        return g1.isLogical() ? -1 : 1;
                    }
                    // leaf first
                    if (g1.childrenNum() != g2.childrenNum()) {
                        return g1.childrenNum() - g2.childrenNum();
                    }
                    // string dict sort
                    return g1.opType.name.compareTo(g2.opType.name);
                })
                .collect(Collectors.toList());

        return PatternGenerator.generateCode(generators, this);
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

    String analyzeClass(Set<String> parentClasses, TypeDeclaration typeDeclaration, TypeType type) {
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
