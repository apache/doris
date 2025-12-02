package org.apache.doris.job.offset.jdbc;

import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.jdbc.split.AbstractSourceSplit;
import lombok.Getter;
import lombok.Setter;
import java.util.Map;

@Getter
@Setter
public class JdbcOffset implements Offset {

    private Map<String, String> meta;

    @Override
    public String toSerializedJson() {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean isValidOffset() {
        return false;
    }

    @Override
    public String showRange() {
        return null;
    }
}
