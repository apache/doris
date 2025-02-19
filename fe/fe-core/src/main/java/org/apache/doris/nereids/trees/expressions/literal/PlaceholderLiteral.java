package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.coercion.PlaceholderDataType;

public class PlaceholderLiteral extends Literal {
    public final int id;

    public PlaceholderLiteral(int id) {
        super(PlaceholderDataType.INSTANCE);
        this.id = id;
    }

    @Override
    public Object getValue() {
        return this;
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PlaceholderLiteral && id == ((PlaceholderLiteral) o).id;
    }

    @Override
    public String computeToSql() {
        return "?";
    }

    @Override
    public String toString() {
        return "?" + id;
    }

    @Override
    protected int computeHashCode() {
        return id;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitPlaceholderLiteral(this, context);
    }
}
