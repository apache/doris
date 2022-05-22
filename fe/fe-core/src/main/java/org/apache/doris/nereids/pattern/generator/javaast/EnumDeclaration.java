package org.apache.doris.nereids.pattern.generator.javaast;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class EnumDeclaration extends TypeDeclaration {
    public final List<TypeType> implementTypes;
    public final List<EnumConstant> constants;

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
