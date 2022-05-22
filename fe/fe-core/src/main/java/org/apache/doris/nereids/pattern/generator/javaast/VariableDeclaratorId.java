package org.apache.doris.nereids.pattern.generator.javaast;

import org.apache.commons.lang3.StringUtils;

public class VariableDeclaratorId implements JavaAstNode {
    public final String identifier;
    public final int arrayDimension;

    public VariableDeclaratorId(String identifier, int arrayDimension) {
        this.identifier = identifier;
        this.arrayDimension = arrayDimension;
    }

    @Override
    public String toString() {
        return identifier + StringUtils.repeat("[]", arrayDimension);
    }
}
