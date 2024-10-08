package org.apache.doris.mtmv;

import com.google.gson.annotations.SerializedName;

public class MTMVPartitionCol {
    @SerializedName("rt")
    private BaseTableInfo relatedTable;

    @SerializedName("pc")
    private String partitionCol;
}
