package org.apache.doris.udf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class MySumDouble {
    public static class State {
        public double counter = 0.0;
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public void add(State state, Double val1, Double val2) {
        if (val1 == null || val2 == null) return;
        state.counter += val1;
        state.counter += val2;
    }

    public void serialize(State state, DataOutputStream out) throws IOException {
        out.writeDouble(state.counter);
    }

    public void deserialize(State state, DataInputStream in) throws IOException {
        state.counter = in.readDouble();
    }

    public void merge(State state, State rhs) {
        state.counter += rhs.counter;
    }

    public Double getValue(State state) {
        return state.counter;
    }
}