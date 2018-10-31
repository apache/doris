package org.apache.doris.journal.bdbje;

import org.apache.doris.common.FeConstants;

import com.google.common.base.Strings;

public class BDBToolOptions {
    private boolean isListDbs;
    private String dbName;
    private boolean isDbStat;
    private boolean hasFromKey;
    private String fromKey;
    private boolean hasEndKey;
    private String endKey;
    private int metaVersion;

    public BDBToolOptions(boolean isListDbs, String dbName, boolean isDbStat,
            String fromKey, String endKey, int metaVersion) {
        this.isListDbs = isListDbs;
        this.dbName = dbName;
        this.isDbStat = isDbStat;
        this.fromKey = fromKey;
        this.hasFromKey = !Strings.isNullOrEmpty(fromKey);
        this.endKey = endKey;
        this.hasEndKey = !Strings.isNullOrEmpty(endKey);
        this.metaVersion = metaVersion == 0 ? FeConstants.meta_version : metaVersion;
    }

    public boolean isListDbs() {
        return isListDbs;
    }

    public String getDbName() {
        return dbName;
    }

    public boolean isDbStat() {
        return isDbStat;
    }

    public boolean hasFromKey() {
        return hasFromKey;
    }

    public String getFromKey() {
        return fromKey;
    }

    public boolean hasEndKey() {
        return hasEndKey;
    }

    public String getEndKey() {
        return endKey;
    }

    public int getMetaVersion() {
        return metaVersion;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("list bdb database: " + isListDbs).append("\n");
        sb.append("bdb database name: " + dbName).append("\n");
        sb.append("get bdb database stat: " + isDbStat).append("\n");
        sb.append("from key" + fromKey).append("\n");
        sb.append("end key: " + endKey).append("\n");
        sb.append("meta version: " + metaVersion).append("\n");
        return sb.toString();
    }
}
