package org.apache.doris.nereids.pattern.generator.javaast;

import java.util.Optional;

public class TypeType implements JavaAstNode {
    public final Optional<ClassOrInterfaceType> classOrInterfaceType;
    public final Optional<String> primitiveType;

    public TypeType(ClassOrInterfaceType classOrInterfaceType, String primitiveType) {
        this.classOrInterfaceType = Optional.ofNullable(classOrInterfaceType);
        this.primitiveType = Optional.ofNullable(primitiveType);
    }

    @Override
    public String toString() {
        if (primitiveType.isPresent()) {
            return primitiveType.get();
        } else {
            return classOrInterfaceType.get().toString();
        }
    }
}
