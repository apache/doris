package org.apache.doris.nereids.pattern.generator.javaast;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class QualifiedName implements JavaAstNode {
    public final List<String> identifiers;

    public QualifiedName(List<String> identifiers) {
        this.identifiers = ImmutableList.copyOf(identifiers);
    }

    public boolean suffixIs(String name) {
        return !identifiers.isEmpty() && identifiers.get(identifiers.size() - 1).equals(name);
    }

    public Optional<String> suffix() {
        if (identifiers.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(identifiers.get(identifiers.size() - 1));
        }
    }

    @Override
    public String toString() {
        return Joiner.on(".").join(identifiers);
    }
}
