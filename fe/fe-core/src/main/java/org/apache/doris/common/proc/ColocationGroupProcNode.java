// ColocationGroupProcNode.java
package org.apache.doris.proc;

import org.apache.doris.catalog.ColocationGroup;
import org.apache.doris.catalog.ColocationGroupMgr;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Proc Node for /colocation_group/{group_id}
 * 展示指定Colocation Group的详细信息
 */
public class ColocationGroupProcNode implements ProcNode {

    // 列名定义（Proc接口返回的表头）
    public static final List<String> TITLE_NAMES = new ArrayList<String>() {
        {
            add("GroupId");
            add("GroupName");
            add("DbId");
            add("TableName");
            add("ReplicaNum");
            add("DistributionCol");
            add("IsStable");
            add("CreateTime");
            add("LastUpdateTime");
        }
    };

    private final long groupId;

    // 构造函数：接收group_id参数
    public ColocationGroupProcNode(long groupId) {
        this.groupId = groupId;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        // 1. 权限校验（Doris标准权限检查）
        if (!ConnectContext.get().getCurrentUser().hasGlobalPriv(PrivPredicate.ADMIN)) {
            throw new AnalysisException("Require ADMIN privilege to access colocation group proc");
        }

        // 2. 获取Colocation Group管理器
        ColocationGroupMgr cgMgr = Env.getCurrentEnv().getColocationGroupMgr();
        ColocationGroup cg = cgMgr.getColocationGroupById(groupId);

        // 3. 校验Group是否存在
        if (cg == null) {
            throw new AnalysisException("Colocation group with id " + groupId + " not found");
        }

        // 4. 构造返回结果
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        
        // 封装Colocation Group核心信息
        List<String> row = new ArrayList<>();
        row.add(String.valueOf(cg.getId()));
        row.add(cg.getName());
        row.add(String.valueOf(cg.getDbId()));
        row.add(String.join(",", cg.getTableIds().stream()
                .map(tableId -> Env.getCurrentEnv().getTable(tableId).getName())
                .toList()));
        row.add(String.valueOf(cg.getReplicaNum()));
        row.add(cg.getDistributionCol());
        row.add(String.valueOf(cg.isStable()));
        row.add(cg.getCreateTime().toString());
        row.add(cg.getLastUpdateTime().toString());
        
        result.addRow(row);
        return result;
    }
}
