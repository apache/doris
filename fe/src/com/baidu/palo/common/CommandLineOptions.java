package com.baidu.palo.common;

import com.baidu.palo.journal.bdbje.BDBToolOptions;

public class CommandLineOptions {

    private boolean isVersion;
    private String helperNode;
    private boolean runBdbTools;
    private BDBToolOptions bdbToolOpts = null;

    public CommandLineOptions(boolean isVersion, String helperNode, BDBToolOptions bdbToolOptions) {
        this.isVersion = isVersion;
        this.helperNode = helperNode;
        this.bdbToolOpts = bdbToolOptions;
        if (this.bdbToolOpts != null) {
            runBdbTools = true;
        } else {
            runBdbTools = false;
        }
    }

    public boolean isVersion() {
        return isVersion;
    }

    public String getHelperNode() {
        return helperNode;
    }

    public boolean runBdbTools() {
        return runBdbTools;
    }

    public BDBToolOptions getBdbToolOpts() {
        return bdbToolOpts;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("print version: " + isVersion).append("\n");
        sb.append("helper node: " + helperNode).append("\n");
        sb.append("bdb tool options: " + bdbToolOpts).append("\n");
        return sb.toString();
    }

}
