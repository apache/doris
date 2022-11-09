package org.apache.doris.udf;
import org.apache.log4j.Logger;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class MyGroupConcatString {
    private static final Logger LOG = Logger.getLogger(MyGroupConcatString.class);
    public static class State {
        public String data = new String();
        public String separator = "-";
        public boolean inited = false;
        String print() {
            String val  = "State data is: ";
            val += (data + " " + separator + " " + inited);
            return val;
        }
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {

    }

    public void add(State state, String val) {
        if (val == null) return;
        if (state.inited) {
            state.data += state.separator;
        } else {
            state.inited = true;
        }
        state.data += val;
    }

    public void serialize(State state, DataOutputStream out) throws IOException {
        out.writeBoolean(state.inited);
        out.writeInt(state.data.length());
        out.writeBytes(state.data);
    }

    public void deserialize(State state, DataInputStream in) throws IOException {
        state.inited = in.readBoolean();
        int len = in.readInt();
        byte[] bytes = new byte[len];
        in.read(bytes);
        state.data = new String(bytes);
    }

    public void merge(State state, State rhs) {
        if (!rhs.inited) {
            return;
        }

        if (!state.inited) {
            state.inited = true;
            state.data = rhs.data;
        } else {
            state.data += state.separator;
            state.data +=rhs.data;
        }
    }

    public String getValue(State state) {
        return state.data;
    }
}