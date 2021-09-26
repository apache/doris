package org.apache.doris.spark.util;

import com.alibaba.fastjson.JSON;
import org.apache.spark.sql.Row;

import java.util.HashMap;


/**
 * @program: incubator-doris
 * @author: huzekang
 * @create: 2021-09-23 18:53
 **/
public class RowUtil {

	public static String convertRow2JsonString(Row row)  {
		final HashMap<String, Object> map = new HashMap<>(row.size());
		final String[] fieldNames = row.schema().fieldNames();
		for (int i = 0; i < row.size(); i++) {
			String columnName = fieldNames[i];
			final Object columnValue = row.get(i);
			map.put(columnName, columnValue);
		}

		return JSON.toJSONString(map);

	}
}
