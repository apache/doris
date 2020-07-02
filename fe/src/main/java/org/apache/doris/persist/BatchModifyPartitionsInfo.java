package org.apache.doris.persist;

import com.google.gson.annotations.SerializedName;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class BatchModifyPartitionsInfo implements Writable {
    @SerializedName(value = "infos")
    private List<ModifyPartitionInfo> infos;

    public BatchModifyPartitionsInfo(List<ModifyPartitionInfo> infos) {
        this.infos = infos;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static BatchModifyPartitionsInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, BatchModifyPartitionsInfo.class);
    }

    public List<ModifyPartitionInfo> getModifyPartitionInfos() {
        return infos;
    }
}
