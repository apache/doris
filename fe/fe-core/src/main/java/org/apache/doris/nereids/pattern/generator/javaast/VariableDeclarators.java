package org.apache.doris.nereids.pattern.generator.javaast;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class VariableDeclarators implements JavaAstNode {
    public final List<VariableDeclarator> variableDeclarators;

    public VariableDeclarators(List<VariableDeclarator> variableDeclarators) {
        this.variableDeclarators = ImmutableList.copyOf(variableDeclarators);
    }

    @Override
    public String toString() {
        return Joiner.on(", ").join(variableDeclarators);
    }
}
