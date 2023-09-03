package org.apache.doris.planner.external;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.statistics.StatisticalType;

import java.util.List;
import java.util.Map;

public class CassandraScanNode extends FileQueryScanNode {
    public CassandraScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                             StatisticalType statisticalType, boolean needCheckColumnPriv) {
        super(id, desc, planNodeName, statisticalType, needCheckColumnPriv);
    }

    @Override
    protected TFileType getLocationType() throws UserException {
        return null;
    }

    @Override
    protected TFileType getLocationType(String location) throws UserException {
        return null;
    }

    @Override
    protected TFileFormatType getFileFormatType() throws UserException {
        return null;
    }

    @Override
    protected List<String> getPathPartitionKeys() throws UserException {
        return null;
    }

    @Override
    protected TableIf getTargetTable() throws UserException {
        return null;
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        return null;
    }


}
