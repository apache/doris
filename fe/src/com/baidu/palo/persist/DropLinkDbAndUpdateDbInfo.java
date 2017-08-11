package com.baidu.palo.persist;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.baidu.palo.catalog.Database.DbState;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;

public class DropLinkDbAndUpdateDbInfo implements Writable {

    private DbState state;
    private LinkDbInfo linkDbInfo;

    public DropLinkDbAndUpdateDbInfo() {
        state = DbState.NORMAL;
        linkDbInfo = new LinkDbInfo();
    }

    public void setUpdateDbState(DbState dbState) {
        this.state = dbState;
    }

    public DbState getUpdateDbState() {
        return state;
    }

    public void setDropDbCluster(String cluster) {
        this.linkDbInfo.setCluster(cluster);
    }

    public String getDropDbCluster() {
        return linkDbInfo.getCluster();
    }

    public void setDropDbName(String db) {
        this.linkDbInfo.setName(db);
    }

    public String getDropDbName() {
        return linkDbInfo.getName();
    }

    public void setDropDbId(long id) {
        this.linkDbInfo.setId(id);
    }

    public long getDropDbId() {
        return linkDbInfo.getId();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, state.toString());
        linkDbInfo.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        state = DbState.valueOf(Text.readString(in));
        linkDbInfo.readFields(in);
    }

}
