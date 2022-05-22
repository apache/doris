package org.apache.doris.nereids.pattern.generator.javaast;

import java.util.Optional;

public class TypeArgument implements JavaAstNode {
    public enum ArgType {
        NORMAL, EXTENDS, SUPER, UNKNOWN
    }

    private final ArgType argType;
    private final Optional<TypeType> typeType;

    public TypeArgument(ArgType argType, TypeType typeType) {
        this.argType = argType;
        this.typeType = Optional.of(typeType);
    }

    @Override
    public String toString() {
        switch (argType) {
            case NORMAL:
                return typeType.get().toString();
            case EXTENDS:
                return "? extends " + typeType.get();
            case SUPER:
                return "? super " + typeType.get();
            case UNKNOWN:
                return "?";
            default:
                throw new UnsupportedOperationException("Unknown argument type: " + argType.toString());
        }
    }
}
