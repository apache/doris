package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.List;
import java.util.TimeZone;

public abstract class DorisCodec {
    protected static String timeZone = "GMT+8";
    protected static TimeZone timeZoner = TimeZone.getTimeZone(timeZone);
    protected final List<String> fieldNames;

    public DorisCodec(final List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    public abstract String serialize(Record row);

    /**
     * convert datax internal  data to string
     *
     * @param col
     * @return
     */
    protected Object convertColumn(final Column col) {
        if (null == col.getRawData()) {
            return null;
        }
        Column.Type type = col.getType();
        switch (type) {
            case BOOL:
            case INT:
            case LONG:
                return col.asLong();
            case DOUBLE:
                return col.asDouble();
            case STRING:
                return col.asString();
            case DATE: {
                final DateColumn.DateType dateType = ((DateColumn) col).getSubType();
                switch (dateType) {
                    case DATE:
                        return DateFormatUtils.format(col.asDate(), "yyyy-MM-dd", timeZoner);
                    case DATETIME:
                        return DateFormatUtils.format(col.asDate(), "yyyy-MM-dd HH:mm:ss", timeZoner);
                    default:
                        return col.asString();
                }
            }
            default:
                // BAD, NULL, BYTES
                return null;
        }
    }
}
