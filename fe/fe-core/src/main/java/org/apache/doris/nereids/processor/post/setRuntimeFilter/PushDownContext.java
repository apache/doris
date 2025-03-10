package org.apache.doris.nereids.processor.post.setRuntimeFilter;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;

import com.alibaba.google.common.collect.ImmutableList;

import java.util.List;

public class PushDownContext {
    private SRFContext sRFContext;
    private PhysicalSetOperation node;
    private Expression source;
    private Expression target;

    public PushDownContext(
            SRFContext srfContext,
            PhysicalSetOperation node,
            Expression source, Expression target) {
        this.sRFContext = srfContext;
        this.node = node;
        this.source = source;
        this.target = target;
    }

    public PhysicalSetOperation getNode() {
        return node;
    }

    public Expression getSource() {
        return source;
    }

    public Expression getTarget() {
        return target;
    }

    public SRFContext getSRFContext() {
        return sRFContext;
    }
}
