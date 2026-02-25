package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.persist.EditLog;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;

public class WorkloadSchedPolicyMgrTest {

    @Mocked
    private Env env;
    
    @Injectable
    private EditLog editLog;

    @Before
    public void setUp() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };
    }

    @Test
    public void testCheckPolicyCondition() {
        WorkloadSchedPolicyMgr mgr = new WorkloadSchedPolicyMgr();

        try {
            List<WorkloadConditionMeta> conditionMetas = new ArrayList<>();
            conditionMetas.add(new WorkloadConditionMeta("username", "=", "user1"));
            conditionMetas.add(new WorkloadConditionMeta("be_scan_rows", ">", "1000"));
            
            List<WorkloadActionMeta> actionMetas = new ArrayList<>();
            actionMetas.add(new WorkloadActionMeta("cancel_query", ""));
            
            mgr.createWorkloadSchedPolicy("policy1", false, conditionMetas, actionMetas, null);
            
        } catch (UserException e) {
            Assert.fail("Should not throw exception for mixed USERNAME and BE metrics: " + e.getMessage());
        }
    }
}
