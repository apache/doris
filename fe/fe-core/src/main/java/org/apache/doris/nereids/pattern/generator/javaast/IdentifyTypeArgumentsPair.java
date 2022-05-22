package org.apache.doris.nereids.pattern.generator.javaast;

import java.util.Optional;

public class IdentifyTypeArgumentsPair implements JavaAstNode {
    public final String identifier;
    public final Optional<TypeArguments> typeArguments;

    public IdentifyTypeArgumentsPair(String identifier, TypeArguments typeArguments) {
        this.identifier = identifier;
        this.typeArguments = Optional.ofNullable(typeArguments);
    }

    @Override
    public String toString() {
        if (typeArguments.isPresent()) {
            return identifier + typeArguments.get();
        } else {
            return identifier;
        }
    }
}
