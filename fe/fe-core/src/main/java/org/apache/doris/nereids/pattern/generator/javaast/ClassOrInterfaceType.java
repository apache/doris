package org.apache.doris.nereids.pattern.generator.javaast;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class ClassOrInterfaceType implements JavaAstNode {
    public final List<IdentifyTypeArgumentsPair> identifyTypeArguments;

    public ClassOrInterfaceType(List<IdentifyTypeArgumentsPair> identifyTypeArguments) {
        this.identifyTypeArguments = ImmutableList.copyOf(identifyTypeArguments);
    }

    @Override
    public String toString() {
        return Joiner.on(".").join(identifyTypeArguments);
    }
}
