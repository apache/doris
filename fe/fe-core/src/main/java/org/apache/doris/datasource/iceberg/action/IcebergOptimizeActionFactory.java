package org.apache.doris.datasource.iceberg.action;

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.action.OptimizeAction;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;

import java.util.Map;
import java.util.Optional;

/**
 * Factory for creating Iceberg-specific OPTIMIZE TABLE actions.
 */
public class IcebergOptimizeActionFactory {

    // Iceberg procedure names (mapped to action types)
    public static final String ROLLBACK_TO_SNAPSHOT = "rollback_to_snapshot";

    /**
     * Create an Iceberg-specific OptimizeAction instance.
     */
    public static OptimizeAction createAction(String actionType, Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            IcebergExternalTable table) throws DdlException {

        switch (actionType.toLowerCase()) {
            case ROLLBACK_TO_SNAPSHOT:
                return new IcebergRollbackToSnapshotAction(properties, partitionNamesInfo,
                        whereCondition, table);
            default:
                throw new DdlException("Unsupported Iceberg procedure: " + actionType
                        + ". Supported procedures: " + String.join(", ", getSupportedActions()));
        }
    }

    /**
     * Check if Iceberg optimize actions are supported for the given table.
     */
    public static boolean isSupported(IcebergExternalTable table) {
        // All Iceberg tables support optimize actions
        return true;
    }

    /**
     * Get supported Iceberg procedure names.
     */
    public static String[] getSupportedActions() {
        return new String[] {
                ROLLBACK_TO_SNAPSHOT
        };
    }
}
