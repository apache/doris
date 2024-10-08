package org.apache.doris.mtmv;

import com.google.gson.annotations.SerializedName;

public class MTMVPartitionCol {
    @SerializedName("rt")
    private BaseTableInfo table;

    @SerializedName("pc")
    private String col;

    public MTMVPartitionCol(BaseTableInfo table, String col) {
        this.table = table;
        this.col = col;
    }

    public BaseTableInfo getTable() {
        return table;
    }

    public String getCol() {
        return col;
    }
}
