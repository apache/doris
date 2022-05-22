package org.apache.doris.nereids.pattern.generator.javaast;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public abstract class TypeDeclaration implements JavaAstNode {
    public final Optional<QualifiedName> packageName;
    public final List<ImportDeclaration> imports;
    public final ClassOrInterfaceModifier modifiers;
    public final String name;
    public final List<TypeDeclaration> children;

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

    public static String getFullQualifiedName(Optional<QualifiedName> packageName, String name) {
        if (packageName.isPresent()) {
            return Joiner.on(".").join(packageName.get().identifiers) + "." + name;
        }
        return name;
    }
}
