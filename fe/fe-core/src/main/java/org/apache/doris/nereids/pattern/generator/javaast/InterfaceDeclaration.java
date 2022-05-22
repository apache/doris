package org.apache.doris.nereids.pattern.generator.javaast;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class InterfaceDeclaration extends TypeDeclaration {
    public final Optional<TypeParameters> typeParameters;
    public final List<TypeType> extendsTypes;

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
        if (packageName.isPresent()) {
            buffer.append("package ").append(packageName.get()).append(";\n\n");
        }

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
        if (typeParameters.isPresent()) {
            buffer.append(typeParameters.get());
        }
        buffer.append(" ");
        if (!extendsTypes.isEmpty()) {
            buffer.append("extends ").append(Joiner.on(", ").join(extendsTypes)).append(" ");
        }
        buffer.append("{}");
        return buffer.toString();
    }
}
