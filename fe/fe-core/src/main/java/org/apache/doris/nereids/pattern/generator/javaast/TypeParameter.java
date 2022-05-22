package org.apache.doris.nereids.pattern.generator.javaast;

import java.util.Optional;

public class TypeParameter implements JavaAstNode {
    public final String identifier;
    public final Optional<TypeBound> typeBound;

    public TypeParameter(String identifier, TypeBound typeBound) {
        this.identifier = identifier;
        this.typeBound = Optional.ofNullable(typeBound);
    }

    @Override
    public String toString() {
        if (typeBound.isPresent()) {
            return identifier + " extends " + typeBound.get();
        } else {
            return identifier;
        }
    }
}
