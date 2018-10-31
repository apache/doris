package com.baidu.palo.transaction;

import com.baidu.palo.persist.EditLog;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mockit.Mock;
import mockit.MockUp;

public final class FakeTransactionIDGenerator extends MockUp<TransactionIDGenerator> {

    private long currentId = 1000L;

    @Mock
    public void $init() {
        // do nothing
    }

    @Mock
    public void setEditLog(EditLog editLog) {
        // do nothing
    }

    @Mock
    public synchronized long getNextTransactionId() {
        System.out.println("getNextTransactionId is called");
        return currentId++;
    }

    @Mock
    public void write(DataOutput out) throws IOException {
        // do nothing
    }

    @Mock
    public void readFields(DataInput in) throws IOException {
        // do nothing
    }

    public void setCurrentId(long newId) {
        this.currentId = newId;
    }
}
