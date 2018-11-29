package org.apache.doris.common.proc;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class SchedulerWorkingSlotsProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("BeId").add("PathHash").add("AvailableSlots").build();

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        List<List<String>> infos = Catalog.getCurrentCatalog().getTabletScheduler().getSlotsInfo();
        result.setRows(infos);
        return result;
    }

}
