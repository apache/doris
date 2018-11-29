package org.apache.doris.common.proc;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.clone.TabletScheduler;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/*
 * show proc "/tablet_scheduler/pending_tablets";
 * show proc "/tablet_scheduler/running_tablets";
 * show proc "/tablet_scheduler/history_tablets";
 */
public class TabletSchedulerProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId").add("Status").add("State").add("OrigPrio").add("DynmPrio")
            .add("SrcBe").add("SrcPath").add("DestBe").add("DestPath").add("Timeout")
            .add("Create").add("Finish").add("FailedSched").add("FailedRunning")
            .add("LastSched").add("LastAdjPrio").add("VisibleVer").add("VisibleVerHash")
            .add("CmtVer").add("CmtVerHash").add("ErrMsg")
            .build();
    
    private String type;
    private TabletScheduler tabletScheduler;

    public TabletSchedulerProcNode(String type) {
        this.type = type;
        tabletScheduler = Catalog.getCurrentCatalog().getTabletScheduler();
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        
        // get at most 1000 tablet infos
        List<List<String>> tabletInfos = Lists.newArrayList();
        if (type.equals(TabletSchedulerProcDir.PENDING_TABLETS)) {
            tabletInfos = tabletScheduler.getPendingTabletsInfo(1000);
        } else if (type.equals(TabletSchedulerProcDir.RUNNING_TABLETS)) {
            tabletInfos = tabletScheduler.getRunningTabletsInfo(1000);
        } else if (type.equals(TabletSchedulerProcDir.HISTORY_TABLETS)) {
            tabletInfos = tabletScheduler.getHistoryTabletsInfo(1000);
        } else {
            throw new AnalysisException("invalid type: " + type);
        }
        result.setRows(tabletInfos);
        return result;
    }

}
