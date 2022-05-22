package org.apache.doris.nereids.pattern.generator.javaast;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class TypeArguments implements JavaAstNode {
    public final List<TypeArgument> typeArguments;

    public TypeArguments(List<TypeArgument> typeArguments) {
        this.typeArguments = ImmutableList.copyOf(typeArguments);
    }

    @Override
    public String toString() {
        return "<" + Joiner.on(", ").join(typeArguments) + ">";
    }
}
