package org.apache.doris.nereids.rules;

import org.apache.doris.nereids.trees.TreeNode;

import java.util.List;

public abstract class Rules {
    protected List<Rule> rules;

    public Rules(List<Rule> rules) {
        this.rules = rules;
    }

    public abstract List<Rule> getCurrentAndChildrenRules(TreeNode<?> treeNode);

    public List<Rule> getCurrentAndChildrenRules() {
        return rules;
    }

    public abstract List<Rule> getCurrentRules(TreeNode<?> treeNode);
}
