package org.apache.doris.tablefunction;

import java.util.List;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.planner.ScanNode;

public abstract class TableFunction {

	public abstract String getTableName();
	
	public abstract List<Column> getTableColumns();
	
	public abstract ScanNode getScanNode();
	
	public Table getTable() {
		Table table = new Table(-1, getTableName(), TableType.TABLE_FUNCTION, getTableColumns());
		return table;
	}
	
	// All table functions should be registered here
	public static TableFunction getTableFunction(String funcName, List<String> params) throws DdlException {
		if (funcName.equalsIgnoreCase(TableFunctionNumbers.NAME)) {
			return new TableFunctionNumbers(params);
		}
		throw new DdlException("Could not find table function " + funcName);
	}
}
