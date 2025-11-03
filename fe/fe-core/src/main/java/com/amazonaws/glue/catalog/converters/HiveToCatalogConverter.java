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
//
// Copied from
// https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore/blob/branch-3.4.0/
//

package com.amazonaws.glue.catalog.converters;

import com.amazonaws.services.glue.model.BinaryColumnStatisticsData;
import com.amazonaws.services.glue.model.BooleanColumnStatisticsData;
import com.amazonaws.services.glue.model.ColumnStatisticsType;
import com.amazonaws.services.glue.model.DateColumnStatisticsData;
import com.amazonaws.services.glue.model.DecimalColumnStatisticsData;
import com.amazonaws.services.glue.model.DoubleColumnStatisticsData;
import com.amazonaws.services.glue.model.LongColumnStatisticsData;
import com.amazonaws.services.glue.model.StringColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HiveToCatalogConverter {

  public static com.amazonaws.services.glue.model.Database convertDatabase(Database hiveDatabase) {
    com.amazonaws.services.glue.model.Database catalogDatabase = new com.amazonaws.services.glue.model.Database();
    catalogDatabase.setName(hiveDatabase.getName());
    catalogDatabase.setDescription(hiveDatabase.getDescription());
    catalogDatabase.setLocationUri(hiveDatabase.getLocationUri());
    catalogDatabase.setParameters(hiveDatabase.getParameters());
    return catalogDatabase;
  }

  public static com.amazonaws.services.glue.model.Table convertTable(
          Table hiveTable) {
    com.amazonaws.services.glue.model.Table catalogTable = new com.amazonaws.services.glue.model.Table();
    catalogTable.setRetention(hiveTable.getRetention());
    catalogTable.setPartitionKeys(convertFieldSchemaList(hiveTable.getPartitionKeys()));
    catalogTable.setTableType(hiveTable.getTableType());
    catalogTable.setName(hiveTable.getTableName());
    catalogTable.setOwner(hiveTable.getOwner());
    catalogTable.setCreateTime(new Date((long) hiveTable.getCreateTime() * 1000));
    catalogTable.setLastAccessTime(new Date((long) hiveTable.getLastAccessTime() * 1000));
    catalogTable.setStorageDescriptor(convertStorageDescriptor(hiveTable.getSd()));
    catalogTable.setParameters(hiveTable.getParameters());
    catalogTable.setViewExpandedText(hiveTable.getViewExpandedText());
    catalogTable.setViewOriginalText(hiveTable.getViewOriginalText());

    return catalogTable;
  }

  public static com.amazonaws.services.glue.model.StorageDescriptor convertStorageDescriptor(
          StorageDescriptor hiveSd) {
    com.amazonaws.services.glue.model.StorageDescriptor catalogSd =
            new com.amazonaws.services.glue.model.StorageDescriptor();
    catalogSd.setNumberOfBuckets(hiveSd.getNumBuckets());
    catalogSd.setCompressed(hiveSd.isCompressed());
    catalogSd.setParameters(hiveSd.getParameters());
    catalogSd.setBucketColumns(hiveSd.getBucketCols());
    catalogSd.setColumns(convertFieldSchemaList(hiveSd.getCols()));
    catalogSd.setInputFormat(hiveSd.getInputFormat());
    catalogSd.setLocation(hiveSd.getLocation());
    catalogSd.setOutputFormat(hiveSd.getOutputFormat());
    catalogSd.setSerdeInfo(convertSerDeInfo(hiveSd.getSerdeInfo()));
    catalogSd.setSkewedInfo(convertSkewedInfo(hiveSd.getSkewedInfo()));
    catalogSd.setSortColumns(convertOrderList(hiveSd.getSortCols()));
    catalogSd.setStoredAsSubDirectories(hiveSd.isStoredAsSubDirectories());

    return catalogSd;
  }

  public static com.amazonaws.services.glue.model.Column convertFieldSchema(
          FieldSchema hiveFieldSchema) {
    com.amazonaws.services.glue.model.Column catalogFieldSchema =
            new com.amazonaws.services.glue.model.Column();
    catalogFieldSchema.setComment(hiveFieldSchema.getComment());
    catalogFieldSchema.setName(hiveFieldSchema.getName());
    catalogFieldSchema.setType(hiveFieldSchema.getType());

    return catalogFieldSchema;
  }

  public static List<com.amazonaws.services.glue.model.Column> convertFieldSchemaList(
          List<FieldSchema> hiveFieldSchemaList) {
    List<com.amazonaws.services.glue.model.Column> catalogFieldSchemaList =
            new ArrayList<com.amazonaws.services.glue.model.Column>();
    for (FieldSchema hiveFs : hiveFieldSchemaList){
      catalogFieldSchemaList.add(convertFieldSchema(hiveFs));
    }

    return catalogFieldSchemaList;
  }

  public static com.amazonaws.services.glue.model.SerDeInfo convertSerDeInfo(
          SerDeInfo hiveSerDeInfo) {
    com.amazonaws.services.glue.model.SerDeInfo catalogSerDeInfo = new com.amazonaws.services.glue.model.SerDeInfo();
    catalogSerDeInfo.setName(hiveSerDeInfo.getName());
    catalogSerDeInfo.setParameters(hiveSerDeInfo.getParameters());
    catalogSerDeInfo.setSerializationLibrary(hiveSerDeInfo.getSerializationLib());

    return catalogSerDeInfo;
  }

  public static com.amazonaws.services.glue.model.SkewedInfo convertSkewedInfo(SkewedInfo hiveSkewedInfo) {
    if (hiveSkewedInfo == null)
      return null;
    com.amazonaws.services.glue.model.SkewedInfo catalogSkewedInfo = new com.amazonaws.services.glue.model.SkewedInfo()
            .withSkewedColumnNames(hiveSkewedInfo.getSkewedColNames())
            .withSkewedColumnValues(convertSkewedValue(hiveSkewedInfo.getSkewedColValues()))
            .withSkewedColumnValueLocationMaps(convertSkewedMap(hiveSkewedInfo.getSkewedColValueLocationMaps()));
    return catalogSkewedInfo;
  }

  public static com.amazonaws.services.glue.model.Order convertOrder(Order hiveOrder) {
    com.amazonaws.services.glue.model.Order order = new com.amazonaws.services.glue.model.Order();
    order.setColumn(hiveOrder.getCol());
    order.setSortOrder(hiveOrder.getOrder());

    return order;
  }

  public static List<com.amazonaws.services.glue.model.Order> convertOrderList(List<Order> hiveOrderList) {
    if (hiveOrderList == null) {
      return null;
    }
    List<com.amazonaws.services.glue.model.Order> catalogOrderList = new ArrayList<>();
    for (Order hiveOrder : hiveOrderList) {
      catalogOrderList.add(convertOrder(hiveOrder));
    }

    return catalogOrderList;
  }

  public static com.amazonaws.services.glue.model.Partition convertPartition(Partition src) {
    com.amazonaws.services.glue.model.Partition tgt = new com.amazonaws.services.glue.model.Partition();

    tgt.setDatabaseName(src.getDbName());
    tgt.setTableName(src.getTableName());
    tgt.setCreationTime(new Date((long) src.getCreateTime() * 1000));
    tgt.setLastAccessTime(new Date((long) src.getLastAccessTime() * 1000));
    tgt.setParameters(src.getParameters());
    tgt.setStorageDescriptor(convertStorageDescriptor(src.getSd()));
    tgt.setValues(src.getValues());

    return tgt;
  }

  public static String convertListToString(final List<String> list) {
    if (list == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < list.size(); i++) {
      String currentString = list.get(i);
      sb.append(currentString.length() + "$" + currentString);
    }

    return sb.toString();
  }

  public static Map<String, String> convertSkewedMap(final Map<List<String>, String> coreSkewedMap){
    if (coreSkewedMap == null){
      return null;
    }
    Map<String, String> catalogSkewedMap = new HashMap<>();
    for (List<String> coreKey : coreSkewedMap.keySet()) {
      catalogSkewedMap.put(convertListToString(coreKey), coreSkewedMap.get(coreKey));
    }
    return catalogSkewedMap;
  }

  public static List<String> convertSkewedValue(final List<List<String>> coreSkewedValue) {
    if (coreSkewedValue == null) {
      return null;
    }
    List<String> catalogSkewedValue = new ArrayList<>();
    for (int i = 0; i < coreSkewedValue.size(); i++) {
      catalogSkewedValue.add(convertListToString(coreSkewedValue.get(i)));
    }

    return catalogSkewedValue;
  }

  public static com.amazonaws.services.glue.model.UserDefinedFunction convertFunction(final Function hiveFunction) {
    if (hiveFunction == null ){
      return null;
    }
    com.amazonaws.services.glue.model.UserDefinedFunction catalogFunction = new com.amazonaws.services.glue.model.UserDefinedFunction();
    catalogFunction.setClassName(hiveFunction.getClassName());
    catalogFunction.setFunctionName(hiveFunction.getFunctionName());
    catalogFunction.setCreateTime(new Date((long) (hiveFunction.getCreateTime()) * 1000));
    catalogFunction.setOwnerName(hiveFunction.getOwnerName());
    if(hiveFunction.getOwnerType() != null) {
      catalogFunction.setOwnerType(hiveFunction.getOwnerType().name());
    }
    catalogFunction.setResourceUris(covertResourceUriList(hiveFunction.getResourceUris()));
    return catalogFunction;
  }

  public static List<com.amazonaws.services.glue.model.ResourceUri> covertResourceUriList(
          final List<ResourceUri> hiveResourceUriList) {
    if (hiveResourceUriList == null) {
      return null;
    }
    List<com.amazonaws.services.glue.model.ResourceUri> catalogResourceUriList = new ArrayList<>();
    for (ResourceUri hiveResourceUri : hiveResourceUriList) {
      com.amazonaws.services.glue.model.ResourceUri catalogResourceUri = new com.amazonaws.services.glue.model.ResourceUri();
      catalogResourceUri.setUri(hiveResourceUri.getUri());
      if (hiveResourceUri.getResourceType() != null) {
        catalogResourceUri.setResourceType(hiveResourceUri.getResourceType().name());
      }
      catalogResourceUriList.add(catalogResourceUri);
    }
    return catalogResourceUriList;
  }

  public static List<com.amazonaws.services.glue.model.ColumnStatistics> convertColumnStatisticsObjList(
          ColumnStatistics hiveColumnStatistics) {
    ColumnStatisticsDesc hiveColumnStatisticsDesc = hiveColumnStatistics.getStatsDesc();
    List<ColumnStatisticsObj> hiveColumnStatisticsObjs = hiveColumnStatistics.getStatsObj();

    List<com.amazonaws.services.glue.model.ColumnStatistics> catalogColumnStatisticsList = new ArrayList<>();
    for (ColumnStatisticsObj hiveColumnStatisticsObj : hiveColumnStatisticsObjs) {
      com.amazonaws.services.glue.model.ColumnStatistics catalogColumnStatistics =
              new com.amazonaws.services.glue.model.ColumnStatistics();
      catalogColumnStatistics.setColumnName(hiveColumnStatisticsObj.getColName());
      catalogColumnStatistics.setColumnType(hiveColumnStatisticsObj.getColType());
      // Last analyzed time in Hive is in days since Epoch, Java Date is in milliseconds
      catalogColumnStatistics.setAnalyzedTime(new Date(TimeUnit.DAYS.toMillis(hiveColumnStatisticsDesc.getLastAnalyzed())));
      catalogColumnStatistics.setStatisticsData(convertColumnStatisticsData(hiveColumnStatisticsObj.getStatsData()));
      catalogColumnStatisticsList.add(catalogColumnStatistics);
    }

    return catalogColumnStatisticsList;
  }

  private static com.amazonaws.services.glue.model.ColumnStatisticsData convertColumnStatisticsData(
          ColumnStatisticsData hiveColumnStatisticsData) {
    com.amazonaws.services.glue.model.ColumnStatisticsData catalogColumnStatisticsData =
            new com.amazonaws.services.glue.model.ColumnStatisticsData();

    // Hive uses the TUnion object to ensure that only one stats object is set at any time, this means that we can
    // only call the get*() of a stats type if the 'setField' is set to that value
    ColumnStatisticsData._Fields setField = hiveColumnStatisticsData.getSetField();
    switch (setField) {
      case BINARY_STATS:
        BinaryColumnStatsData hiveBinaryData = hiveColumnStatisticsData.getBinaryStats();
        BinaryColumnStatisticsData catalogBinaryData = new BinaryColumnStatisticsData();
        catalogBinaryData.setNumberOfNulls(hiveBinaryData.getNumNulls());
        catalogBinaryData.setMaximumLength(hiveBinaryData.getMaxColLen());
        catalogBinaryData.setAverageLength(hiveBinaryData.getAvgColLen());
        catalogColumnStatisticsData.setType(String.valueOf(ColumnStatisticsType.BINARY));
        catalogColumnStatisticsData.setBinaryColumnStatisticsData(catalogBinaryData);
        break;

      case BOOLEAN_STATS:
        BooleanColumnStatsData hiveBooleanData = hiveColumnStatisticsData.getBooleanStats();
        BooleanColumnStatisticsData catalogBooleanData = new BooleanColumnStatisticsData();
        catalogBooleanData.setNumberOfNulls(hiveBooleanData.getNumNulls());
        catalogBooleanData.setNumberOfFalses(hiveBooleanData.getNumFalses());
        catalogBooleanData.setNumberOfTrues(hiveBooleanData.getNumTrues());
        catalogColumnStatisticsData.setType(String.valueOf(ColumnStatisticsType.BOOLEAN));
        catalogColumnStatisticsData.setBooleanColumnStatisticsData(catalogBooleanData);
        break;

      case DATE_STATS:
        DateColumnStatsData hiveDateData = hiveColumnStatisticsData.getDateStats();
        DateColumnStatisticsData catalogDateData = new DateColumnStatisticsData();
        catalogDateData.setNumberOfNulls(hiveDateData.getNumNulls());
        catalogDateData.setNumberOfDistinctValues(hiveDateData.getNumDVs());
        catalogDateData.setMaximumValue(ConverterUtils.hiveDatetoDate(hiveDateData.getHighValue()));
        catalogDateData.setMinimumValue(ConverterUtils.hiveDatetoDate(hiveDateData.getLowValue()));
        catalogColumnStatisticsData.setType(String.valueOf(ColumnStatisticsType.DATE));
        catalogColumnStatisticsData.setDateColumnStatisticsData(catalogDateData);
        break;

      case DECIMAL_STATS:
        DecimalColumnStatsData hiveDecimalData = hiveColumnStatisticsData.getDecimalStats();
        DecimalColumnStatisticsData catalogDecimalData = new DecimalColumnStatisticsData();
        catalogDecimalData.setNumberOfNulls(hiveDecimalData.getNumNulls());
        catalogDecimalData.setNumberOfDistinctValues(hiveDecimalData.getNumDVs());
        catalogDecimalData.setMaximumValue(convertDecimal(hiveDecimalData.getHighValue()));
        catalogDecimalData.setMinimumValue(convertDecimal(hiveDecimalData.getLowValue()));
        catalogColumnStatisticsData.setType(String.valueOf(ColumnStatisticsType.DECIMAL));
        catalogColumnStatisticsData.setDecimalColumnStatisticsData(catalogDecimalData);
        break;

      case DOUBLE_STATS:
        DoubleColumnStatsData hiveDoubleData = hiveColumnStatisticsData.getDoubleStats();
        DoubleColumnStatisticsData catalogDoubleData = new DoubleColumnStatisticsData();
        catalogDoubleData.setNumberOfNulls(hiveDoubleData.getNumNulls());
        catalogDoubleData.setNumberOfDistinctValues(hiveDoubleData.getNumDVs());
        catalogDoubleData.setMaximumValue(hiveDoubleData.getHighValue());
        catalogDoubleData.setMinimumValue(hiveDoubleData.getLowValue());
        catalogColumnStatisticsData.setType(String.valueOf(ColumnStatisticsType.DOUBLE));
        catalogColumnStatisticsData.setDoubleColumnStatisticsData(catalogDoubleData);
        break;
      case LONG_STATS:
        LongColumnStatsData hiveLongData = hiveColumnStatisticsData.getLongStats();
        LongColumnStatisticsData catalogLongData = new LongColumnStatisticsData();
        catalogLongData.setNumberOfNulls(hiveLongData.getNumNulls());
        catalogLongData.setNumberOfDistinctValues(hiveLongData.getNumDVs());
        catalogLongData.setMaximumValue(hiveLongData.getHighValue());
        catalogLongData.setMinimumValue(hiveLongData.getLowValue());
        catalogColumnStatisticsData.setType(String.valueOf(ColumnStatisticsType.LONG));
        catalogColumnStatisticsData.setLongColumnStatisticsData(catalogLongData);
        break;

      case STRING_STATS:
        StringColumnStatsData hiveStringData = hiveColumnStatisticsData.getStringStats();
        StringColumnStatisticsData catalogStringData = new StringColumnStatisticsData();
        catalogStringData.setNumberOfNulls(hiveStringData.getNumNulls());
        catalogStringData.setNumberOfDistinctValues(hiveStringData.getNumDVs());
        catalogStringData.setMaximumLength(hiveStringData.getMaxColLen());
        catalogStringData.setAverageLength(hiveStringData.getAvgColLen());
        catalogColumnStatisticsData.setType(String.valueOf(ColumnStatisticsType.STRING));
        catalogColumnStatisticsData.setStringColumnStatisticsData(catalogStringData);
        break;
    }

    return catalogColumnStatisticsData;
  }

  private static com.amazonaws.services.glue.model.DecimalNumber convertDecimal(Decimal hiveDecimal) {
    com.amazonaws.services.glue.model.DecimalNumber catalogDecimal =
            new com.amazonaws.services.glue.model.DecimalNumber();
    catalogDecimal.setUnscaledValue(ByteBuffer.wrap(hiveDecimal.getUnscaled()));
    catalogDecimal.setScale((int)hiveDecimal.getScale());
    return catalogDecimal;
  }

}
