package org.apache.doris.meta;

public class MetaContext {

    private int journalVersion;

    private static ThreadLocal<MetaContext> threadLocalInfo = new ThreadLocal<MetaContext>();

    public MetaContext() {

    }

    public void setJournalVersion(int journalVersion) {
        this.journalVersion = journalVersion;
    }

    public int getJournalVersion() {
        return journalVersion;
    }

    public void setThreadLocalInfo() {
        threadLocalInfo.set(this);
    }
    
    public static MetaContext get() {
        return threadLocalInfo.get();
    }

    public static void remove() {
        threadLocalInfo.remove();
    }
}
