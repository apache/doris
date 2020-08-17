package org.apache.doris.load.loadv2;

import org.apache.doris.common.LoadException;

public interface ConfigFile {
    public void createFile() throws LoadException;
    public String getFilePath();
}
