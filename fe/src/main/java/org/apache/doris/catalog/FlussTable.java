
package org.apache.doris.catalog;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class FlussTable extends ExternalTable implements Writable {

    private String flussStreamName;

    public FlussTable() {
        super(TableType.FLUSS);
    }

    public FlussTable(long id, String name, Map<String, String> properties) {
        super(id, name, TableType.FLUSS);
        this.flussStreamName = properties.getOrDefault("fluss.stream", "default_stream");
    }

    public String getFlussStreamName() {
        return flussStreamName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, flussStreamName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        this.flussStreamName = Text.readString(in);
    }
}
