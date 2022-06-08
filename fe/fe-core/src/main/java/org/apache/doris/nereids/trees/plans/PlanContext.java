package org.apache.doris.nereids.trees.plans;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;

import com.clearspring.analytics.util.Lists;

import java.util.List;

public class PlanContext {
    private List<PlanFragment> planFragmentList = Lists.newArrayList();

    private DescriptorTable descTable = new DescriptorTable();


    private final IdGenerator<PlanFragmentId> fragmentIdGenerator = PlanFragmentId.createGenerator();

    private final IdGenerator<PlanNodeId> nodeIdGenerator = PlanNodeId.createGenerator();

    public List<PlanFragment> getPlanFragmentList() {
        return planFragmentList;
    }

    public TupleDescriptor generateTupleDesc() {
        return descTable.createTupleDescriptor();
    }

    public PlanNodeId nextNodeId() {
        return nodeIdGenerator.getNextId();
    }

    public SlotDescriptor addSlotDesc(TupleDescriptor t) {
        return descTable.addSlotDescriptor(t);
    }

    public SlotDescriptor addSlotDesc(TupleDescriptor t, int id) {
        return descTable.addSlotDescriptor(t, id);
    }

    public PlanFragmentId nextFragmentId() {
        return fragmentIdGenerator.getNextId();
    }

    public void addPlanFragment(PlanFragment planFragment) {
        this.planFragmentList.add(planFragment);
    }

}
