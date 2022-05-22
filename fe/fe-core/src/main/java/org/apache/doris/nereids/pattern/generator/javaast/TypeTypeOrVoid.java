package org.apache.doris.nereids.pattern.generator.javaast;

import java.util.Optional;

public class TypeTypeOrVoid implements JavaAstNode {
    public final Optional<TypeType> typeType;
    public final boolean isVoid;

    public TypeTypeOrVoid(TypeType typeType, boolean isVoid) {
        this.typeType = Optional.ofNullable(typeType);
        this.isVoid = isVoid;
    }

    @Override
    public String toString() {
        return isVoid ? "void" : typeType.get().toString();
    }
}
