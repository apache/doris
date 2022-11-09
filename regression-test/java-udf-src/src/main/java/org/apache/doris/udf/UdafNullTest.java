package org.apache.doris.udf;
import org.apache.log4j.Logger;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class UdafNullTest {
    private static final Logger LOG = Logger.getLogger(UdafNullTest.class);
    public static class State {
        public long counter = 0;
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public void add(State state, Integer val) {
        if (val == null) return;
        state.counter += val;
    }

    public void serialize(State state, DataOutputStream out) throws IOException {
        out.writeLong(state.counter);
    }

    public void deserialize(State state, DataInputStream in) throws IOException {
        state.counter = in.readLong();
    }

    public void merge(State state, State rhs) {
        state.counter += rhs.counter;
    }

    public Long getValue(State state) {
        return null;
    }
}