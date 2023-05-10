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
import com.amazonaws.services.glue.model.ColumnStatistics;
import com.amazonaws.services.glue.model.ColumnStatisticsType;
import com.amazonaws.services.glue.model.DateColumnStatisticsData;
import com.amazonaws.services.glue.model.DecimalColumnStatisticsData;
import com.amazonaws.services.glue.model.DoubleColumnStatisticsData;
import com.amazonaws.services.glue.model.ErrorDetail;
import com.amazonaws.services.glue.model.LongColumnStatisticsData;
import com.amazonaws.services.glue.model.StringColumnStatisticsData;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.log4j.Logger;
import shade.doris.hive.org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class BaseCatalogToHiveConverter implements CatalogToHiveConverter {

  private static final Logger logger = Logger.getLogger(BaseCatalogToHiveConverter.class);

  private static final ImmutableMap<String, HiveException> EXCEPTION_MAP = ImmutableMap.<String, HiveException>builder()
      .put("AlreadyExistsException", new HiveException() {
        public TException get(String msg) {
          return new AlreadyExistsException(msg);
        }
      })
      .put("InvalidInputException", new HiveException() {
        public TException get(String msg) {
          return new InvalidObjectException(msg);
        }
      })
      .put("InternalServiceException", new HiveException() {
        public TException get(String msg) {
          return new MetaException(msg);
        }
      })
      .put("ResourceNumberLimitExceededException", new HiveException() {
        public TException get(String msg) {
          return new MetaException(msg);
        }
      })
      .put("OperationTimeoutException", new HiveException() {
        public TException get(String msg) {
          return new MetaException(msg);
        }
      })
      .put("EntityNotFoundException", new HiveException() {
        public TException get(String msg) {
          return new NoSuchObjectException(msg);
        }
      })
      .build();

  interface HiveException {
    TException get(String msg);
  }

  public TException wrapInHiveException(Throwable e) {
    return getHiveException(e.getClass().getSimpleName(), e.getMessage());
  }

  public TException errorDetailToHiveException(ErrorDetail errorDetail) {
    return getHiveException(errorDetail.getErrorCode(), errorDetail.getErrorMessage());
  }

  private TException getHiveException(String errorName, String errorMsg) {
    if (EXCEPTION_MAP.containsKey(errorName)) {
      return EXCEPTION_MAP.get(errorName).get(errorMsg);
    } else {
      logger.warn("Hive Exception type not found for " + errorName);
      return new MetaException(errorMsg);
    }
  }

  public Database convertDatabase(com.amazonaws.services.glue.model.Database catalogDatabase) {
    Database hiveDatabase = new Database();
    hiveDatabase.setName(catalogDatabase.getName());
    hiveDatabase.setDescription(catalogDatabase.getDescription());
    String location = catalogDatabase.getLocationUri();
    hiveDatabase.setLocationUri(location == null ? "" : location);
    hiveDatabase.setParameters(firstNonNull(catalogDatabase.getParameters(), Maps.<String, String>newHashMap()));
    return hiveDatabase;
  }

  public FieldSchema convertFieldSchema(com.amazonaws.services.glue.model.Column catalogFieldSchema) {
    FieldSchema hiveFieldSchema = new FieldSchema();
    hiveFieldSchema.setType(catalogFieldSchema.getType());
    hiveFieldSchema.setName(catalogFieldSchema.getName());
    hiveFieldSchema.setComment(catalogFieldSchema.getComment());

    return hiveFieldSchema;
  }

  public List<FieldSchema> convertFieldSchemaList(List<com.amazonaws.services.glue.model.Column> catalogFieldSchemaList) {
    List<FieldSchema> hiveFieldSchemaList = new ArrayList<>();
    if (catalogFieldSchemaList == null) {
      return hiveFieldSchemaList;
    }
    for (com.amazonaws.services.glue.model.Column catalogFieldSchema : catalogFieldSchemaList){
      hiveFieldSchemaList.add(convertFieldSchema(catalogFieldSchema));
    }

    return hiveFieldSchemaList;
  }

  public Table convertTable(com.amazonaws.services.glue.model.Table catalogTable, String dbname) {
    Table hiveTable = new Table();
    hiveTable.setDbName(dbname);
    hiveTable.setTableName(catalogTable.getName());
    Date createTime = catalogTable.getCreateTime();
    hiveTable.setCreateTime(createTime == null ? 0 : (int) (createTime.getTime() / 1000));
    hiveTable.setOwner(catalogTable.getOwner());
    Date lastAccessedTime = catalogTable.getLastAccessTime();
    hiveTable.setLastAccessTime(lastAccessedTime == null ? 0 : (int) (lastAccessedTime.getTime() / 1000));
    hiveTable.setRetention(catalogTable.getRetention());
    hiveTable.setSd(convertStorageDescriptor(catalogTable.getStorageDescriptor()));
    hiveTable.setPartitionKeys(convertFieldSchemaList(catalogTable.getPartitionKeys()));
    // Hive may throw a NPE during dropTable if the parameter map is null.
    Map<String, String> parameterMap = catalogTable.getParameters();
    if (parameterMap == null) {
      parameterMap = Maps.newHashMap();
    }
    hiveTable.setParameters(parameterMap);
    hiveTable.setViewOriginalText(catalogTable.getViewOriginalText());
    hiveTable.setViewExpandedText(catalogTable.getViewExpandedText());
    hiveTable.setTableType(catalogTable.getTableType());

    return hiveTable;
  }

  public TableMeta convertTableMeta(com.amazonaws.services.glue.model.Table catalogTable, String dbName) {
    TableMeta tableMeta = new TableMeta();
    tableMeta.setDbName(dbName);
    tableMeta.setTableName(catalogTable.getName());
    tableMeta.setTableType(catalogTable.getTableType());
    if (catalogTable.getParameters().containsKey("comment")) {
      tableMeta.setComments(catalogTable.getParameters().get("comment"));
    }
    return tableMeta;
  }

  public StorageDescriptor convertStorageDescriptor(com.amazonaws.services.glue.model.StorageDescriptor catalogSd) {
    StorageDescriptor hiveSd = new StorageDescriptor();
    hiveSd.setCols(convertFieldSchemaList(catalogSd.getColumns()));
    hiveSd.setLocation(catalogSd.getLocation());
    hiveSd.setInputFormat(catalogSd.getInputFormat());
    hiveSd.setOutputFormat(catalogSd.getOutputFormat());
    hiveSd.setCompressed(catalogSd.getCompressed());
    hiveSd.setNumBuckets(catalogSd.getNumberOfBuckets());
    hiveSd.setSerdeInfo(convertSerDeInfo(catalogSd.getSerdeInfo()));
    hiveSd.setBucketCols(firstNonNull(catalogSd.getBucketColumns(), Lists.<String>newArrayList()));
    hiveSd.setSortCols(convertOrderList(catalogSd.getSortColumns()));
    hiveSd.setParameters(firstNonNull(catalogSd.getParameters(), Maps.<String, String>newHashMap()));
    hiveSd.setSkewedInfo(convertSkewedInfo(catalogSd.getSkewedInfo()));
    hiveSd.setStoredAsSubDirectories(catalogSd.getStoredAsSubDirectories());

    return hiveSd;
  }

  public Order convertOrder(com.amazonaws.services.glue.model.Order catalogOrder) {
    Order hiveOrder = new Order();
    hiveOrder.setCol(catalogOrder.getColumn());
    hiveOrder.setOrder(catalogOrder.getSortOrder());

    return hiveOrder;
  }

  public List<Order> convertOrderList(List<com.amazonaws.services.glue.model.Order> catalogOrderList) {
    List<Order> hiveOrderList = new ArrayList<>();
    if (catalogOrderList == null) {
      return hiveOrderList;
    }
    for (com.amazonaws.services.glue.model.Order catalogOrder : catalogOrderList){
      hiveOrderList.add(convertOrder(catalogOrder));
    }

    return hiveOrderList;
  }

  public SerDeInfo convertSerDeInfo(com.amazonaws.services.glue.model.SerDeInfo catalogSerDeInfo){
    SerDeInfo hiveSerDeInfo = new SerDeInfo();
    hiveSerDeInfo.setName(catalogSerDeInfo.getName());
    hiveSerDeInfo.setParameters(firstNonNull(catalogSerDeInfo.getParameters(), Maps.<String, String>newHashMap()));
    hiveSerDeInfo.setSerializationLib(catalogSerDeInfo.getSerializationLibrary());

    return hiveSerDeInfo;
  }

  public SkewedInfo convertSkewedInfo(com.amazonaws.services.glue.model.SkewedInfo catalogSkewedInfo) {
    if (catalogSkewedInfo == null) {
      return null;
    }
    
    SkewedInfo hiveSkewedInfo = new SkewedInfo();
    hiveSkewedInfo.setSkewedColNames(firstNonNull(catalogSkewedInfo.getSkewedColumnNames(), Lists.<String>newArrayList()));
    hiveSkewedInfo.setSkewedColValues(convertSkewedValue(catalogSkewedInfo.getSkewedColumnValues()));
    hiveSkewedInfo.setSkewedColValueLocationMaps(convertSkewedMap(catalogSkewedInfo.getSkewedColumnValueLocationMaps()));
    return hiveSkewedInfo;
  }

  public Partition convertPartition(com.amazonaws.services.glue.model.Partition src) {
	  Partition tgt = new Partition();
	  Date createTime = src.getCreationTime();
	  if (createTime != null) {
		  tgt.setCreateTime((int) (createTime.getTime() / 1000)); 
		  tgt.setCreateTimeIsSet(true);
	  } else {
		  tgt.setCreateTimeIsSet(false);
	  }
	  String dbName = src.getDatabaseName();
	  if (dbName != null) {
		  tgt.setDbName(dbName);
		  tgt.setDbNameIsSet(true);
	  } else {
		  tgt.setDbNameIsSet(false);
	  }
	  Date lastAccessTime = src.getLastAccessTime();
	  if (lastAccessTime != null) {
		  tgt.setLastAccessTime((int) (lastAccessTime.getTime() / 1000));
		  tgt.setLastAccessTimeIsSet(true);
	  } else {
		  tgt.setLastAccessTimeIsSet(false);
	  }
	  Map<String, String> params = src.getParameters();
	  
	  // A null parameter map causes Hive to throw a NPE
	  // so ensure we do not return a Partition object with a null parameter map.
	  if (params == null) {
	    params = Maps.newHashMap();
	  }
	  
	  tgt.setParameters(params);
	  tgt.setParametersIsSet(true);
	  
	  String tableName = src.getTableName();
	  if (tableName != null) {
		  tgt.setTableName(tableName);
		  tgt.setTableNameIsSet(true);
	  } else {
		  tgt.setTableNameIsSet(false);
	  }
	  
	  List<String> values = src.getValues();
	  if (values != null) {
		  tgt.setValues(values);
		  tgt.setValuesIsSet(true);
	  } else {
		  tgt.setValuesIsSet(false);
	  }
	  
	  com.amazonaws.services.glue.model.StorageDescriptor sd = src.getStorageDescriptor();
	  if (sd != null) {
		  StorageDescriptor hiveSd = convertStorageDescriptor(sd);
		  tgt.setSd(hiveSd);
		  tgt.setSdIsSet(true);
	  } else {
		  tgt.setSdIsSet(false);
	  }
	  
	  return tgt;
  }

  public List<Partition> convertPartitions(List<com.amazonaws.services.glue.model.Partition> src) {
    if (src == null) {
      return null;
    }

    List<Partition> target = Lists.newArrayList();
    for (com.amazonaws.services.glue.model.Partition partition : src) {
      target.add(convertPartition(partition));
    }
    return target;
  }

  public List<String> convertStringToList(final String s) {
    if (s == null) {
      return null;
    }
    List<String> listString = new ArrayList<>();
    for (int i = 0; i < s.length();) {
      StringBuilder length = new StringBuilder();
      for (int j = i; j < s.length(); j++){
        if (s.charAt(j) != '$') {
          length.append(s.charAt(j));
        } else {
          int lengthOfString = Integer.valueOf(length.toString());
          listString.add(s.substring(j + 1, j + 1 + lengthOfString));
          i = j + 1 + lengthOfString;
          break;
        }
      }
    }
    return listString;
  }

  @Nonnull
  public Map<List<String>, String> convertSkewedMap(final @Nullable Map<String, String> catalogSkewedMap) {
    Map<List<String>, String> skewedMap = new HashMap<>();
    if (catalogSkewedMap == null){
      return skewedMap;
    }

    for (String coralKey : catalogSkewedMap.keySet()) {
      skewedMap.put(convertStringToList(coralKey), catalogSkewedMap.get(coralKey));
    }
    return skewedMap;
  }

  @Nonnull
  public List<List<String>> convertSkewedValue(final @Nullable List<String> catalogSkewedValue) {
    List<List<String>> skewedValues = new ArrayList<>();
    if (catalogSkewedValue == null){
      return skewedValues;
    }

    for (String skewValue : catalogSkewedValue) {
      skewedValues.add(convertStringToList(skewValue));
    }
    return skewedValues;
  }
  
  public PrincipalType convertPrincipalType(com.amazonaws.services.glue.model.PrincipalType catalogPrincipalType) {
    if(catalogPrincipalType == null) {
      return null;
    }
    
    if(catalogPrincipalType == com.amazonaws.services.glue.model.PrincipalType.GROUP) {
      return PrincipalType.GROUP;
    } else if(catalogPrincipalType == com.amazonaws.services.glue.model.PrincipalType.USER) {
      return PrincipalType.USER;
    } else if(catalogPrincipalType == com.amazonaws.services.glue.model.PrincipalType.ROLE) {
      return PrincipalType.ROLE;
    }
    throw new RuntimeException("Unknown principal type:" + catalogPrincipalType.name());
  }

  public Function convertFunction(final String dbName,
                                  final com.amazonaws.services.glue.model.UserDefinedFunction catalogFunction) {
    if (catalogFunction ==  null) {
      return null;
    }
    Function hiveFunction = new Function();
    hiveFunction.setClassName(catalogFunction.getClassName());
    if (catalogFunction.getCreateTime() != null) {
      //AWS Glue can return function with null create time
      hiveFunction.setCreateTime((int) (catalogFunction.getCreateTime().getTime() / 1000));
    }
    hiveFunction.setDbName(dbName);
    hiveFunction.setFunctionName(catalogFunction.getFunctionName());
    hiveFunction.setFunctionType(FunctionType.JAVA);
    hiveFunction.setOwnerName(catalogFunction.getOwnerName());
    hiveFunction.setOwnerType(convertPrincipalType(com.amazonaws.services.glue.model.PrincipalType.fromValue(catalogFunction.getOwnerType())));
    hiveFunction.setResourceUris(convertResourceUriList(catalogFunction.getResourceUris()));
    return hiveFunction;
  }

  public List<ResourceUri> convertResourceUriList(
          final List<com.amazonaws.services.glue.model.ResourceUri> catalogResourceUriList) {
    if (catalogResourceUriList == null) {
      return null;
    }
    List<ResourceUri> hiveResourceUriList = new ArrayList<>();
    for (com.amazonaws.services.glue.model.ResourceUri catalogResourceUri : catalogResourceUriList) {
      ResourceUri hiveResourceUri = new ResourceUri();
      hiveResourceUri.setUri(catalogResourceUri.getUri());
      if (catalogResourceUri.getResourceType() != null) {
        hiveResourceUri.setResourceType(ResourceType.valueOf(catalogResourceUri.getResourceType()));
      }
      hiveResourceUriList.add(hiveResourceUri);
    }

    return hiveResourceUriList;
  }

  public List<ColumnStatisticsObj> convertColumnStatisticsList(List<ColumnStatistics> catatlogColumnStatisticsList) {
    List<ColumnStatisticsObj> hiveColumnStatisticsList = new ArrayList<>();
    for (ColumnStatistics catalogColumnStatistics : catatlogColumnStatisticsList) {
      ColumnStatisticsObj hiveColumnStatistics = new ColumnStatisticsObj();
      hiveColumnStatistics.setColName(catalogColumnStatistics.getColumnName());
      hiveColumnStatistics.setColType(catalogColumnStatistics.getColumnType());
      hiveColumnStatistics.setStatsData(convertColumnStatisticsData(catalogColumnStatistics.getStatisticsData()));
      hiveColumnStatisticsList.add(hiveColumnStatistics);
    }

    return hiveColumnStatisticsList;
  }

  private ColumnStatisticsData convertColumnStatisticsData(
      com.amazonaws.services.glue.model.ColumnStatisticsData catalogColumnStatisticsData) {
    ColumnStatisticsData hiveColumnStatisticsData = new ColumnStatisticsData();

    ColumnStatisticsType type = ColumnStatisticsType.fromValue(catalogColumnStatisticsData.getType());
    switch (type) {
      case BINARY:
        BinaryColumnStatisticsData catalogBinaryData = catalogColumnStatisticsData.getBinaryColumnStatisticsData();
        BinaryColumnStatsData hiveBinaryData = new BinaryColumnStatsData();
        hiveBinaryData.setAvgColLen(catalogBinaryData.getAverageLength());
        hiveBinaryData.setMaxColLen(catalogBinaryData.getMaximumLength());
        hiveBinaryData.setNumNulls(catalogBinaryData.getNumberOfNulls());

        hiveColumnStatisticsData.setFieldValue(ColumnStatisticsData._Fields.BINARY_STATS, hiveBinaryData);
        hiveColumnStatisticsData.setBinaryStats(hiveBinaryData);
        break;

      case BOOLEAN:
        BooleanColumnStatisticsData catalogBooleanData = catalogColumnStatisticsData.getBooleanColumnStatisticsData();
        BooleanColumnStatsData hiveBooleanData = new BooleanColumnStatsData();
        hiveBooleanData.setNumFalses(catalogBooleanData.getNumberOfFalses());
        hiveBooleanData.setNumTrues(catalogBooleanData.getNumberOfTrues());
        hiveBooleanData.setNumNulls(catalogBooleanData.getNumberOfNulls());

        hiveColumnStatisticsData.setBooleanStats(hiveBooleanData);
        break;

      case DATE:
        DateColumnStatisticsData catalogDateData = catalogColumnStatisticsData.getDateColumnStatisticsData();
        DateColumnStatsData hiveDateData = new DateColumnStatsData();
        hiveDateData.setLowValue(ConverterUtils.dateToHiveDate(catalogDateData.getMinimumValue()));
        hiveDateData.setHighValue(ConverterUtils.dateToHiveDate(catalogDateData.getMaximumValue()));
        hiveDateData.setNumDVs(catalogDateData.getNumberOfDistinctValues());
        hiveDateData.setNumNulls(catalogDateData.getNumberOfNulls());

        hiveColumnStatisticsData.setDateStats(hiveDateData);
        break;

      case DECIMAL:
        DecimalColumnStatisticsData catalogDecimalData = catalogColumnStatisticsData.getDecimalColumnStatisticsData();
        DecimalColumnStatsData hiveDecimalData = new DecimalColumnStatsData();
        hiveDecimalData.setLowValue(convertDecimal(catalogDecimalData.getMinimumValue()));
        hiveDecimalData.setHighValue(convertDecimal(catalogDecimalData.getMaximumValue()));
        hiveDecimalData.setNumDVs(catalogDecimalData.getNumberOfDistinctValues());
        hiveDecimalData.setNumNulls(catalogDecimalData.getNumberOfNulls());

        hiveColumnStatisticsData.setDecimalStats(hiveDecimalData);
        break;

      case DOUBLE:
        DoubleColumnStatisticsData catalogDoubleData = catalogColumnStatisticsData.getDoubleColumnStatisticsData();
        DoubleColumnStatsData hiveDoubleData = new DoubleColumnStatsData();
        hiveDoubleData.setLowValue(catalogDoubleData.getMinimumValue());
        hiveDoubleData.setHighValue(catalogDoubleData.getMaximumValue());
        hiveDoubleData.setNumDVs(catalogDoubleData.getNumberOfDistinctValues());
        hiveDoubleData.setNumNulls(catalogDoubleData.getNumberOfNulls());

        hiveColumnStatisticsData.setDoubleStats(hiveDoubleData);
        break;

      case LONG:
        LongColumnStatisticsData catalogLongData = catalogColumnStatisticsData.getLongColumnStatisticsData();
        LongColumnStatsData hiveLongData = new LongColumnStatsData();
        hiveLongData.setLowValue(catalogLongData.getMinimumValue());
        hiveLongData.setHighValue(catalogLongData.getMaximumValue());
        hiveLongData.setNumDVs(catalogLongData.getNumberOfDistinctValues());
        hiveLongData.setNumNulls(catalogLongData.getNumberOfNulls());

        hiveColumnStatisticsData.setLongStats(hiveLongData);
        break;

      case STRING:
        StringColumnStatisticsData catalogStringData = catalogColumnStatisticsData.getStringColumnStatisticsData();
        StringColumnStatsData hiveStringData = new StringColumnStatsData();
        hiveStringData.setAvgColLen(catalogStringData.getAverageLength());
        hiveStringData.setMaxColLen(catalogStringData.getMaximumLength());
        hiveStringData.setNumDVs(catalogStringData.getNumberOfDistinctValues());
        hiveStringData.setNumNulls(catalogStringData.getNumberOfNulls());

        hiveColumnStatisticsData.setStringStats(hiveStringData);
        break;
    }

    return hiveColumnStatisticsData;
  }

  private Decimal convertDecimal(com.amazonaws.services.glue.model.DecimalNumber catalogDecimal) {
    Decimal hiveDecimal = new Decimal();
    hiveDecimal.setUnscaled(catalogDecimal.getUnscaledValue());
    hiveDecimal.setScale(catalogDecimal.getScale().shortValue());
    return hiveDecimal;
  }

}
