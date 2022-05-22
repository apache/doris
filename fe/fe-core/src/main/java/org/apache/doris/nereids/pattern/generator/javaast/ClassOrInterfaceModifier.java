package org.apache.doris.nereids.pattern.generator.javaast;

import com.google.common.base.Joiner;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

public class ClassOrInterfaceModifier implements JavaAstNode {
    public final int mod;

    public ClassOrInterfaceModifier(int mod) {
        this.mod = mod;
    }

    @Override
    public String toString() {
        List<String> modifiers = new ArrayList<>(3);
        if (Modifier.isPublic(mod)) {
            modifiers.add("public");
        } else if (Modifier.isProtected(mod)) {
            modifiers.add("protected");
        } else if (Modifier.isPrivate(mod)) {
            modifiers.add("private");
        }

        if (Modifier.isStatic(mod)) {
            modifiers.add("static");
        }

        if (Modifier.isAbstract(mod)) {
            modifiers.add("abstract");
        } else if (Modifier.isFinal(mod)) {
            modifiers.add("final");
        }
        return Joiner.on(" ").join(modifiers);
    }
}
