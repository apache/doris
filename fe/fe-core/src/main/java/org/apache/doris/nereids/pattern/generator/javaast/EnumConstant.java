package org.apache.doris.nereids.pattern.generator.javaast;

public class EnumConstant implements JavaAstNode {
    public final String identifier;

    public EnumConstant(String identifier) {
        this.identifier = identifier;
    }

    @Override
    public String toString() {
        return identifier;
    }
}
