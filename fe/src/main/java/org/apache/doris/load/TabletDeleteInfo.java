package org.apache.doris.load;

import com.google.common.collect.Sets;
import org.apache.doris.catalog.Replica;
import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

public class TabletDeleteInfo implements Writable {
    private long tabletId;
    private Set<Replica> finishedReplicas;

    public TabletDeleteInfo() {
        // for persist
    }

    public TabletDeleteInfo(long tabletId) {
        this.tabletId = tabletId;
        this.finishedReplicas = Sets.newHashSet();
    }

    public long getTabletId() {
        return tabletId;
    }

    public Set<Replica> getFinishedReplicas() {
        return finishedReplicas;
    }

    public boolean addFinishedReplica(Replica replica) {
        finishedReplicas.add(replica);
        return true;
    }

    public static TabletDeleteInfo read(DataInput in) throws IOException {
        TabletDeleteInfo tabletDeleteInfo = new TabletDeleteInfo();
        tabletDeleteInfo.readFields(in);
        return tabletDeleteInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(tabletId);
        short size = (short) finishedReplicas.size();
        out.writeShort(size);
        for (Replica replica : finishedReplicas) {
            replica.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        tabletId = in.readLong();
        short size = in.readShort();
        for (short i = 0; i < size; i++) {
            Replica replica = Replica.read(in);
            finishedReplicas.add(replica);
        }
    }
}
