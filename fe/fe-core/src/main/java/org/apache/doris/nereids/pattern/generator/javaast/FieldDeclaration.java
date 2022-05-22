package org.apache.doris.nereids.pattern.generator.javaast;

public class FieldDeclaration extends MemberDeclaration {
    public final TypeType type;
    public final VariableDeclarators variableDeclarators;

    public FieldDeclaration(TypeType type, VariableDeclarators variableDeclarators) {
        this.type = type;
        this.variableDeclarators = variableDeclarators;
    }

    @Override
    public String toString() {
        return type + " " + variableDeclarators + ";";
    }
}
