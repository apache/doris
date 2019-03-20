package org.apache.doris.optimizer.base;

/**
 * for controlling searching.
 */
public class SearchVariable {

    private boolean isExecuteOptimization;

    public SearchVariable() {
        this.isExecuteOptimization = true;
    }

    public void setExecuteOptimization(boolean value) {
        this.isExecuteOptimization = value;
    }

    public boolean isExecuteOptimization() {
        return this.isExecuteOptimization;
    }
}
