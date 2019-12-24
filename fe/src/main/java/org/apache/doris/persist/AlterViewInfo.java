package org.apache.doris.persist;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AlterViewInfo implements Writable {
    private long dbId;
    private long tableId;

    private String inlineViewDef;

    public AlterViewInfo() {
        // for persist
    }

    public AlterViewInfo(long dbId, long tableId, String inlineViewDef) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.inlineViewDef = inlineViewDef;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        Text.writeString(out, inlineViewDef);
    }

    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        inlineViewDef = Text.readString(in);
    }

    public static AlterViewInfo read(DataInput in) throws IOException {
        AlterViewInfo info = new AlterViewInfo();
        info.readFields(in);
        return info;
    }
}
