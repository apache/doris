package org.apache.doris.persist;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReplaceTableOperationLog implements Writable {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "origTblId")
    private long origTblId;
    @SerializedName(value = "newTblName")
    private long newTblId;
    @SerializedName(value = "swapTable")
    private boolean swapTable;

    public ReplaceTableOperationLog(long dbId, long origTblId, long newTblId, boolean swapTable) {
        this.dbId = dbId;
        this.origTblId = origTblId;
        this.newTblId = newTblId;
        this.swapTable = swapTable;
    }

    public long getDbId() {
        return dbId;
    }

    public long getOrigTblId() {
        return origTblId;
    }

    public long getNewTblId() {
        return newTblId;
    }

    public boolean isSwapTable() {
        return swapTable;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ReplaceTableOperationLog read(DataInput in) throws  IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ReplaceTableOperationLog.class);
    }
}
