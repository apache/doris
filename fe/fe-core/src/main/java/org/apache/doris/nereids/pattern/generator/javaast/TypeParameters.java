package org.apache.doris.nereids.pattern.generator.javaast;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class TypeParameters implements JavaAstNode {
    public final List<TypeParameter> typeParameters;

    public TypeParameters(List<TypeParameter> typeParameters) {
        this.typeParameters = ImmutableList.copyOf(typeParameters);
    }

    @Override
    public String toString() {
        return "<" + Joiner.on(", ").join(typeParameters) + ">";
    }
}
