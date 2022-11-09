package org.apache.doris.udf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class MySumFloat {
    public static class State {
        public float counter = 0;
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public void add(State state, Float val1) {
        if (val1 == null) return;
        state.counter += val1;
    }

    public void serialize(State state, DataOutputStream out) throws IOException {
        out.writeFloat(state.counter);
    }

    public void deserialize(State state, DataInputStream in) throws IOException {
        state.counter = in.readFloat();
    }

    public void merge(State state, State rhs) {
        state.counter += rhs.counter;
    }

    public Float getValue(State state) {
        return state.counter;
    }
}