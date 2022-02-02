package org.apache.doris.tablefunction;

import java.util.ArrayList;
import java.util.List;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.planner.ScanNode;

// Table function that generate int64 numbers
// have a single column number
public class TableFunctionNumbers extends TableFunction {

	public static final String NAME = "numbers";
	
	public TableFunctionNumbers(List<String> params) {
		// Only have a single parameter Number of rows to generate
	}

	@Override
	public String getTableName() {
		return "TableFunctionNumbers";
	}

	@Override
	public List<Column> getTableColumns() {
		List<Column> resColumns = new ArrayList<>();
		resColumns.add(new Column("number", PrimitiveType.BIGINT, false));
		return resColumns;
	}

	@Override
	public ScanNode getScanNode() {
		// TODO Auto-generated method stub
		return null;
	}

}
