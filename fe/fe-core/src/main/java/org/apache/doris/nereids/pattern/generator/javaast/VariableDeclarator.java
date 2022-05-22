package org.apache.doris.nereids.pattern.generator.javaast;

public class VariableDeclarator implements JavaAstNode {
    public final VariableDeclaratorId variableDeclaratorId;

    public VariableDeclarator(VariableDeclaratorId variableDeclaratorId) {
        this.variableDeclaratorId = variableDeclaratorId;
    }

    @Override
    public String toString() {
        return variableDeclaratorId.toString();
    }
}
