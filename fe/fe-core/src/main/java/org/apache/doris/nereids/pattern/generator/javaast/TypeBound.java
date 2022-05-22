package org.apache.doris.nereids.pattern.generator.javaast;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class TypeBound implements JavaAstNode {
    public final List<TypeType> types;

    public TypeBound(List<TypeType> types) {
        this.types = ImmutableList.copyOf(types);
    }

    @Override
    public String toString() {
        return Joiner.on(" & ").join(types);
    }
}
