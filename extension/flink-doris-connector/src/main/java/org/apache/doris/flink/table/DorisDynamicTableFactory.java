// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.table;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * The {@link DorisDynamicTableFactory} translates the catalog table to a table source.
 *
 * <p>Because the table source requires a decoding format, we are discovering the format using the
 * provided {@link FactoryUtil} for convenience.
 */
public final class DorisDynamicTableFactory implements  DynamicTableSourceFactory , DynamicTableSinkFactory {

	public static final ConfigOption<String> FENODES = ConfigOptions.key("fenodes").stringType().noDefaultValue().withDescription("doris fe http address.");
	public static final ConfigOption<String> TABLE_IDENTIFIER = ConfigOptions.key("table.identifier").stringType().noDefaultValue().withDescription("the jdbc table name.");
	public static final ConfigOption<String> USERNAME = ConfigOptions.key("username").stringType().noDefaultValue().withDescription("the jdbc user name.");
	public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password").stringType().noDefaultValue().withDescription("the jdbc password.");

	@Override
	public String factoryIdentifier() {
		return "doris"; // used for matching to `connector = '...'`
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(FENODES);
		options.add(TABLE_IDENTIFIER);
		options.add(USERNAME);
		options.add(PASSWORD);

		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		return options;
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		// either implement your custom validation logic here ...
		// or use the provided helper utility
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		// validate all options
		helper.validate();

		// get the validated options
		final ReadableConfig options = helper.getOptions();
		// derive the produced data type (excluding computed columns) from the catalog table
		final DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		// create and return dynamic table source
		return new DorisDynamicTableSource(getDorisOptions(helper.getOptions()),physicalSchema);
	}

	private DorisOptions getDorisOptions(ReadableConfig readableConfig) {
		final String fenodes = readableConfig.get(FENODES);
		final DorisOptions.Builder builder = DorisOptions.builder()
				.setFenodes(fenodes)
				.setTableIdentifier(readableConfig.get(TABLE_IDENTIFIER));

		readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
		readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
		return builder.build();
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		// either implement your custom validation logic here ...
		// or use the provided helper utility
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		// validate all options
		helper.validate();
		// create and return dynamic table source
		return new DorisDynamicTableSink(getDorisOptions(helper.getOptions()));
	}
}
