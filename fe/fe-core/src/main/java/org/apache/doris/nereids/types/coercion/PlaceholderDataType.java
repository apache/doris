package org.apache.doris.nereids.types.coercion;

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.types.DataType;

public class PlaceholderDataType extends DataType {
    public static final PlaceholderDataType INSTANCE = new PlaceholderDataType();

    private PlaceholderDataType() {}

    @Override
    public Type toCatalogDataType() {
        return null;
    }

    @Override
    public String toSql() {
        return "?";
    }

    @Override
    public int width() {
        return 0;
    }
}
