package org.apache.doris.nereids.pattern;

import org.apache.doris.nereids.operators.Operator;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.trees.TreeNode;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public class TypePattern<T extends TreeNode> extends Pattern<T> {
    protected final Class<T> type;

    public TypePattern(Class<T> clazz, Pattern... children) {
        super(OperatorType.NORMAL_PATTERN, children);
        this.type = Objects.requireNonNull(clazz, "class can not be null");
    }

    public TypePattern(Class<T> clazz, List<Predicate<T>> predicates, Pattern... children) {
        super(OperatorType.NORMAL_PATTERN, predicates, children);
        this.type = Objects.requireNonNull(clazz, "class can not be null");
    }

    @Override
    protected boolean doMatchRoot(T root) {
        return type.isInstance(root) && predicates.stream().allMatch(predicate -> predicate.test(root));
    }

    @Override
    public TypePattern<T> withPredicates(List<Predicate<T>> predicates) {
        return new TypePattern(type, predicates, children.toArray(new Pattern[0]));
    }

    @Override
    public boolean matchOperator(Operator operator) {
        return type.isInstance(operator);
    }
}
