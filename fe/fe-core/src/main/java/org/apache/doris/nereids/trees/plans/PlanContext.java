package org.apache.doris.nereids.trees.plans;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;

import java.util.List;

public class PlanContext {
    private List<PlanFragment> planFragmentList;

    private Analyzer analyzer;

    private final IdGenerator<PlanFragmentId> fragmentIdGenerator = PlanFragmentId.createGenerator();

    private final IdGenerator<PlanNodeId> nodeIdGenerator = PlanNodeId.createGenerator();

    public List<PlanFragment> getPlanFragmentList() {
        return planFragmentList;
    }

    public TupleDescriptor generateTupleDesc() {
        return analyzer.getDescTbl().createTupleDescriptor();
    }

    public PlanNodeId nextNodeId() {
        return nodeIdGenerator.getNextId();
    }

    public SlotDescriptor addSlotDesc(TupleDescriptor t) {
        return analyzer.getDescTbl().addSlotDescriptor(t);
    }

    public PlanFragmentId nextFragmentId() {
        return fragmentIdGenerator.getNextId();
    }

    public void addPlanFragment(PlanFragment planFragment) {
        this.planFragmentList.add(planFragment);
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }
}
