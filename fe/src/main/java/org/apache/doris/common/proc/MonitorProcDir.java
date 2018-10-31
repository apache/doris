package org.apache.doris.common.proc;

import org.apache.doris.common.AnalysisException;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

public class MonitorProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("Info")
            .build();

    public MonitorProcDir() {

    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<String> jvmRow = Lists.newArrayList();
        jvmRow.add("jvm");
        jvmRow.add(" ");
        result.addRow(jvmRow);

        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String name) throws AnalysisException {
        if (Strings.isNullOrEmpty(name)) {
            throw new AnalysisException("name is null");
        }

        if (name.equalsIgnoreCase("jvm")) {
            return new JvmProcDir();
        } else {
            throw new AnalysisException("unknown name: " + name);
        }
    }

}
