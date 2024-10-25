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

package org.pentaho.di.trans.steps.dorisstreamloader;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.AfterInjection;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisDataType;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisJdbcConnectionOptions;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisJdbcConnectionProvider;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisQueryVisitor;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.*;

/**
 * Here are the steps that we need to take to make streaming loading possible for MySQL:<br>
 * <br>
 * The following steps are carried out by the step at runtime:<br>
 * <br>
 * - create a unique FIFO file (using mkfifo, LINUX ONLY FOLKS!)<br>
 * - Create a target table using standard Kettle SQL generation<br>
 * - Execute the LOAD DATA SQL Command to bulk load in a separate SQL thread in the background:<br>
 * - Write to the FIFO file<br>
 * - At the end, close the output stream to the FIFO file<br>
 * * At the end, remove the FIFO file <br>
 *
 *
 * Created on 24-oct-2007<br>
 *
 * @author Matt Casters<br>
 */
@Step( id = "DorisStreamLoaderStep", name = "BaseStep.TypeLongDesc.DorisKettleConnector",
  description = "BaseStep.TypeTooltipDesc.DorisStreamLoader",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Bulk",
  image = "BLKMYSQL.svg",
  documentationUrl = "http://wiki.pentaho.com/display/EAI/MySQL+Bulk+Loader",
  i18nPackageName = "org.pentaho.di.trans.steps.dorisstreamloader" )
@InjectionSupported( localizationPrefix = "DorisStreamLoader.Injection.", groups = { "FIELDS" } )
public class DorisStreamLoaderMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = DorisStreamLoaderMeta.class; // for i18n purposes, needed by Translator2!!

  @Injection( name = "JDCB_URL" )
  private String jdbcUrl;

  @Injection( name = "HTTP_URL" )
  private String fenodes;

  /** The name of the FIFO file to create */
  @Injection( name = "DATABASE" )
  private String database;

  @Injection( name = "TABLE" )
  private String table;

  @Injection(name = "USERNAME")
  private String username;

  @Injection(name = "PASSWORD")
  private String password;

  private Properties streamLoadProp;

  private long bufferFlushMaxRows;

  private long bufferFlushMaxBytes;

  private long bufferFlushIntervalMs;

  private int maxRetries;

  /** Field name of the target table */
  @Injection( name = "FIELD_TABLE", group = "FIELDS" )
  private String[] fieldTable;

  /** Field name in the stream */
  @Injection( name = "FIELD_STREAM", group = "FIELDS" )
  private String[] fieldStream;

  private DorisQueryVisitor dorisQueryVisitor;


  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode, databases );
  }

  private void readData( Node stepnode, List<? extends SharedObjectInterface> databases ) throws KettleXMLException {
    try {
      fenodes =XMLHandler.getTagValue(stepnode,"httpUrl");
      jdbcUrl=XMLHandler.getTagValue(stepnode,"jdbcUrl");
      database=XMLHandler.getTagValue(stepnode,"database");
      table=XMLHandler.getTagValue(stepnode,"table");
      username=XMLHandler.getTagValue(stepnode,"username");
      password=XMLHandler.getTagValue(stepnode,"password");
      bufferFlushMaxBytes= Long.parseLong(XMLHandler.getTagValue(stepnode,"bufferFlushMaxBytes"));
      bufferFlushMaxRows= Long.parseLong(XMLHandler.getTagValue(stepnode,"bufferFlushMaxRows"));
      maxRetries= Integer.parseInt(XMLHandler.getTagValue(stepnode,"maxRetries"));

      String propertiesString = XMLHandler.getTagValue(stepnode, "streamLoadProp");
      if (propertiesString != null && !propertiesString.isEmpty()) {
        this.streamLoadProp = new Properties();
        StringReader reader = new StringReader(propertiesString);
        this.streamLoadProp.load(reader);
      } else {
        this.streamLoadProp = new Properties();
      }


      // Field data mapping
      int nrvalues = XMLHandler.countNodes(stepnode, "mapping");
      allocate(nrvalues);
      for (int i = 0; i < nrvalues; i++) {
        Node vnode = XMLHandler.getSubNodeByNr(stepnode, "mapping", i);

        fieldTable[i] = XMLHandler.getTagValue(vnode, "stream_name");
        fieldStream[i] = XMLHandler.getTagValue(vnode, "field_name");
        if (fieldStream[i] == null) {
          fieldStream[i] = fieldTable[i]; // default: the same name!
        }
      }

    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString( PKG,
          "DorisStreamLoaderMeta.Exception.UnableToReadStepInfoFromXML" ), e );
    }
  }

  public void setDefault() {
    fieldTable = null;
    jdbcUrl=null;
    fenodes =null;
    database=null;
    table=null;
    username="root";
    password="";
    streamLoadProp=null;
    bufferFlushMaxRows=10000L;
    bufferFlushMaxBytes=1073741824L;
    bufferFlushIntervalMs=1000L;
    maxRetries=3;
    allocate(0);
  }

  public void allocate(int nrvalues) {
    fieldTable = new String[nrvalues];
    fieldStream = new String[nrvalues];
  }

  public Object clone() {
    DorisStreamLoaderMeta retval = (DorisStreamLoaderMeta) super.clone();
    int nrvalues = fieldTable.length;
    retval.allocate(nrvalues);
    System.arraycopy(fieldTable, 0, retval.fieldTable, 0, nrvalues);
    System.arraycopy(fieldStream, 0, retval.fieldStream, 0, nrvalues);

    return retval;
  }

  @Override
  public String getDialogClassName() {
    return super.getDialogClassName();
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append("    ").append(XMLHandler.addTagValue("jdbcUrl", jdbcUrl));
    retval.append("    ").append(XMLHandler.addTagValue("httpUrl", fenodes));
    retval.append("    ").append(XMLHandler.addTagValue("database", database));
    retval.append("    ").append(XMLHandler.addTagValue("table", table));
    retval.append("    ").append(XMLHandler.addTagValue("username", username));
    retval.append("    ").append(XMLHandler.addTagValue("password", password));
    // 将 Properties 对象转换为字符串并写入 XML
    if (this.streamLoadProp != null) {
      try {
        StringWriter writer = new StringWriter();
        this.streamLoadProp.store(writer, null); // null 表示没有注释
        String propertiesString = writer.toString();

        // 将 Properties 的字符串形式写入 XML 节点
        retval.append("    ").append(XMLHandler.addTagValue("streamLoadProp", propertiesString));
      } catch (Exception e) {
        throw new RuntimeException("Error converting properties to XML", e);
      }
    }

    retval.append("    ").append(XMLHandler.addTagValue("bufferFlushMaxRows", bufferFlushMaxRows));
    retval.append("    ").append(XMLHandler.addTagValue("bufferFlushMaxBytes", bufferFlushMaxBytes));
    retval.append("    ").append(XMLHandler.addTagValue("maxRetries", maxRetries));

    for (int i = 0; i < fieldTable.length; i++) {
      retval.append("      <mapping>").append(Const.CR);
      retval.append("        ").append(XMLHandler.addTagValue("stream_name", fieldTable[i]));
      retval.append("        ").append(XMLHandler.addTagValue("field_name", fieldStream[i]));
      retval.append("      </mapping>").append(Const.CR);
    }

    return retval.toString();
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {

      jdbcUrl=rep.getStepAttributeString(id_step,"jdbcUrl");
      fenodes =rep.getStepAttributeString(id_step,"httpUrl");
      database=rep.getStepAttributeString(id_step,"database");
      table=rep.getStepAttributeString(id_step,"table");
      username=rep.getStepAttributeString(id_step,"username");
      password=rep.getStepAttributeString(id_step,"password");

      // 读取 Properties 对象 (以字符串存储)
      String propertiesString = rep.getStepAttributeString(id_step, "streamLoadProp");

      if (propertiesString != null && !propertiesString.isEmpty()) {
        // 将字符串转换为 Properties 对象
        this.streamLoadProp = new Properties();
        StringReader reader = new StringReader(propertiesString);
        this.streamLoadProp.load(reader);
      }




      bufferFlushMaxRows= Long.parseLong(rep.getStepAttributeString(id_step,"bufferFlushMaxRows"));
      bufferFlushMaxBytes= Long.parseLong(rep.getStepAttributeString(id_step,"bufferFlushMaxBytes"));
      maxRetries= Integer.parseInt(rep.getStepAttributeString(id_step,"maxRetries"));

      int nrvalues = rep.countNrStepAttributes(id_step, "stream_name");

      allocate(nrvalues);

      for (int i = 0; i < nrvalues; i++) {
        fieldTable[i] = rep.getStepAttributeString(id_step, i, "stream_name");
        fieldStream[i] = rep.getStepAttributeString(id_step, i, "field_name");
        if (fieldStream[i] == null) {
          fieldStream[i] = fieldTable[i];
        }
      }

    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString( PKG,
          "DorisStreamLoaderMeta.Exception.UnexpectedErrorReadingStepInfoFromRepository" ), e );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {

      rep.saveStepAttribute(id_transformation,id_step,"httpurl", fenodes);
      rep.saveStepAttribute(id_transformation,id_step,"jdbcurl",jdbcUrl);
      rep.saveStepAttribute(id_transformation,id_step,"database",database);
      rep.saveStepAttribute(id_transformation,id_step,"table",table);
      rep.saveStepAttribute(id_transformation,id_step,"username",username);
      rep.saveStepAttribute(id_transformation,id_step,"password",password);

      // 将 Properties 对象转换为字符串并保存
      if (this.streamLoadProp != null) {
        StringWriter writer = new StringWriter();
        this.streamLoadProp.store(writer, null); // null 表示没有注释
        String propertiesString = writer.toString();
        // 保存 Properties 字符串到存储库
        rep.saveStepAttribute(id_transformation, id_step, "streamLoadProp", propertiesString);
      }

      rep.saveStepAttribute(id_transformation,id_step,"bufferFlushMaxBytes",bufferFlushMaxBytes);
      rep.saveStepAttribute(id_transformation,id_step,"bufferFlushMaxRows",bufferFlushMaxRows);
      rep.saveStepAttribute(id_transformation,id_step,"maxRetries",maxRetries);

      for (int i = 0; i < fieldTable.length; i++) {
        rep.saveStepAttribute(id_transformation, id_step, i, "stream_name", fieldTable[i]);
        rep.saveStepAttribute(id_transformation, id_step, i, "field_name", fieldStream[i]);
      }

    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString( PKG,
          "DorisStreamLoaderMeta.Exception.UnableToSaveStepInfoToRepository" )
          + id_step, e );
    }
  }

  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
    // Default: nothing changes to rowMeta
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository,
      IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    if (jdbcUrl!=null){
      try {
        if (dorisQueryVisitor == null){
          DorisJdbcConnectionOptions dorisJdbcConnectionOptions = new DorisJdbcConnectionOptions(this.jdbcUrl, this.username, this.password);
          DorisJdbcConnectionProvider dorisJdbcConnectionProvider = new DorisJdbcConnectionProvider(dorisJdbcConnectionOptions);
          dorisQueryVisitor = new DorisQueryVisitor(dorisJdbcConnectionProvider,this.database,this.table);
        }

        if (!Utils.isEmpty(table)){
          cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK,BaseMessages.getString(PKG,
                  "DorisKettleConnectorMeta.CheckResult.TableNameOK"),stepMeta);
          remarks.add(cr);

          try {
            if (!dorisQueryVisitor.getAllTables().contains(this.table)){
              error_message = BaseMessages.getString(PKG,"DorisKettleConnectorMeta.CheckResult.NoNeedTable")+table;
              cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,error_message,stepMeta);
            }
          }catch (Exception e){
            error_message = BaseMessages.getString(PKG, "DorisKettleConnectorMeta.CheckResult.ErrorConnJDBC") + e.getMessage();
            cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
            remarks.add(cr);
          }
        }

        // Check fields in table
        boolean first = true;
        boolean error_found = false;
        error_message = "";

        Map<String, DorisDataType> fielsMap = dorisQueryVisitor.getFieldMapping();
        if (fielsMap != null) {
          cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "DorisKettleConnectorMeta.CheckResult.TableExists"), stepMeta);
          remarks.add(cr);

          // How about the fields to insert/dateMask in the table?
          first = true;
          error_found = false;
          error_message = "";

          for (int i = 0; i < fieldTable.length; i++) {
            String field = fieldTable[i];

            boolean isFieldExists = fielsMap.containsKey(field);
            if (!isFieldExists) {
              if (first) {
                first = false;
                error_message += BaseMessages.getString(PKG, "DorisKettleConnectorMeta.CheckResult.MissingFieldsToLoadInTargetTable") + Const.CR;
              }
              error_found = true;
              error_message += "\t\t" + field + Const.CR;
            }

          }
          if (error_found) {
            cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
          } else {
            cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "DorisKettleConnectorMeta.CheckResult.AllFieldsFoundInTargetTable"), stepMeta);
          }
          remarks.add(cr);
        }

        // Look up fields in the input stream <prev>
        if (prev != null && prev.size() > 0) {
          cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "DorisKettleConnectorMeta.CheckResult.StepReceivingDatas", prev.size() + ""), stepMeta);
          remarks.add(cr);

          first = true;
          error_found = false;
          error_message = "";

          for (int i = 0; i < fieldStream.length; i++) {
            ValueMetaInterface v = prev.searchValueMeta(fieldStream[i]);
            if (v == null) {
              if (first) {
                first = false;
                error_message +=
                        BaseMessages.getString(PKG, "DorisKettleConnectorMeta.CheckResult.MissingFieldsInInput") + Const.CR;
              }
              error_found = true;
              error_message += "\t\t" + fieldStream[i] + Const.CR;
            }
          }
          if (error_found) {
            cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
          } else {
            cr =
                    new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
                            "DorisKettleConnectorMeta.CheckResult.AllFieldsFoundInInput"), stepMeta);
          }
          remarks.add(cr);
        } else {
          error_message = BaseMessages.getString(PKG, "DorisKettleConnectorMeta.CheckResult.MissingFieldsInInput3") + Const.CR;
          cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
          remarks.add(cr);
        }


        // Check the Kettle-Doris Type Mapping.
        error_found = false;
        first = true;
        error_message = "";
        for (int i = 0; i < fieldStream.length; i++) {
          ValueMetaInterface v = prev.searchValueMeta(fieldStream[i]);
          DorisDataType type = fielsMap.get(fieldTable[i]);
          if (!isCorrectTypeMapping(v.getType(), type)) {
            if (first) {
              first = false;
              error_message += BaseMessages.getString(PKG, "DorisKettleConnectorMeta.CheckResult.ErrorTypeMapping") + Const.CR;
            }
            error_found = true;
            error_message += "\t\t" + ValueMetaInterface.getTypeDescription(v.getType()) + "---->" + type.toString() + Const.CR;
          }
        }
        if (error_found) {
          cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
        } else {
          cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "DorisKettleConnectorMeta.CheckResult.CorrectTypeMapping"), stepMeta);
        }
        remarks.add(cr);

      } catch (Exception e) {
        error_message = BaseMessages.getString(PKG, "DorisKettleConnectorMeta.CheckResult.DatabaseErrorOccurred") + e.getMessage();
        cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
        remarks.add(cr);
      }
    } else {
      cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "DorisKettleConnectorMeta.CheckResult.NoJDBCUrl"), stepMeta);
      remarks.add(cr);
    }


    // See if we have input streams leading to this step!
    if (input.length > 0) {
      cr =
              new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
                      "DorisKettleConnectorMeta.CheckResult.StepReceivingInfoFromOtherSteps"), stepMeta);
      remarks.add(cr);
    } else {
      cr =
              new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG,
                      "StarRocksKettleConnectorMeta.CheckResult.NoInputError"), stepMeta);
      remarks.add(cr);
    }
  }

private boolean isCorrectTypeMapping(int kettleType, DorisDataType starrocksType) {
  if (starrocksType == null) {
    return false;
  }
  switch (kettleType) {
    case ValueMetaInterface.TYPE_NUMBER:
    case ValueMetaInterface.TYPE_STRING:
    case ValueMetaInterface.TYPE_DATE:
    case ValueMetaInterface.TYPE_BOOLEAN:
    case ValueMetaInterface.TYPE_INTEGER:
    case ValueMetaInterface.TYPE_BIGNUMBER:
    case ValueMetaInterface.TYPE_TIMESTAMP:
    case ValueMetaInterface.TYPE_INET:
      if (typeMapping.get(kettleType).contains(starrocksType)) {
        return true;
      }
    case ValueMetaInterface.TYPE_BINARY:
      logError(BaseMessages.getString(PKG, "DorisKettleConnectorMeta.Message.UnSupportBinary"));
    case ValueMetaInterface.TYPE_SERIALIZABLE:
      logError(BaseMessages.getString(PKG, "DorisKettleConnectorMeta.Message.UnSupportSerializable"));
    default:
      logError(BaseMessages.getString(PKG, "DorisKettleConnectorMeta.Message.UnknowType"));
  }
  return false;
}

private Map<Integer, List<DorisDataType>> typeMapping = new HashMap<Integer, List<DorisDataType>>() {
  {
    put(ValueMetaInterface.TYPE_NUMBER, Arrays.asList(DorisDataType.DOUBLE, DorisDataType.FLOAT));
    put(ValueMetaInterface.TYPE_STRING, Arrays.asList(DorisDataType.VARCHAR, DorisDataType.CHAR, DorisDataType.STRING, DorisDataType.JSON));
    put(ValueMetaInterface.TYPE_DATE, Arrays.asList(DorisDataType.DATE, DorisDataType.DATETIME));
    put(ValueMetaInterface.TYPE_BOOLEAN, Arrays.asList(DorisDataType.BOOLEAN, DorisDataType.TINYINT));
    put(ValueMetaInterface.TYPE_INTEGER, Arrays.asList(DorisDataType.TINYINT, DorisDataType.SMALLINT, DorisDataType.INT, DorisDataType.BIGINT));
    put(ValueMetaInterface.TYPE_BIGNUMBER, Arrays.asList(DorisDataType.LARGEINT, DorisDataType.DECIMAL,DorisDataType.UNKNOWN));
    put(ValueMetaInterface.TYPE_TIMESTAMP, Arrays.asList(DorisDataType.DATETIME, DorisDataType.DATE));
    put(ValueMetaInterface.TYPE_INET, Arrays.asList(DorisDataType.STRING));
  }
};

  public boolean containsString(String[] array, String a) {
    for (String element : array) {
      if (element.equals(a)) {
        return true;
      }
    }
    return false;
  }

  public boolean isOpAutoProjectionInJson() {
    String version = getDorisQueryVisitor().getStarRocksVersion();
    return version == null || version.length() > 0 && !version.trim().startsWith("1.");
  }

  @AfterInjection
  public void afterInjectionSynchronization() {
    int nrFields = (fieldTable == null) ? -1 : fieldTable.length;
    if (nrFields <= 0) {
      return;
    }
    String[][] rtnStrings = Utils.normalizeArrays(nrFields, fieldStream);
    fieldStream = rtnStrings[0];

  }


  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
      Trans trans ) {
    return new DorisStreamLoader( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new DorisStreamLoaderData();
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  public String getFenodes() {
    return fenodes;
  }

  public void setFenodes(String fenodes) {
    this.fenodes = fenodes;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public Properties getStreamLoadProp() {
    return streamLoadProp;
  }

  public void setStreamLoadProp(Properties streamLoadProp) {
    this.streamLoadProp = streamLoadProp;
  }

  public long getBufferFlushMaxRows() {
    return bufferFlushMaxRows;
  }

  public void setBufferFlushMaxRows(long bufferFlushMaxRows) {
    this.bufferFlushMaxRows = bufferFlushMaxRows;
  }

  public long getBufferFlushMaxBytes() {
    return bufferFlushMaxBytes;
  }

  public void setBufferFlushMaxBytes(long bufferFlushMaxBytes) {
    this.bufferFlushMaxBytes = bufferFlushMaxBytes;
  }

  public long getBufferFlushIntervalMs() {
    return bufferFlushIntervalMs;
  }

  public void setBufferFlushIntervalMs(long bufferFlushIntervalMs) {
    this.bufferFlushIntervalMs = bufferFlushIntervalMs;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  public String[] getFieldTable() {
    return fieldTable;
  }

  public void setFieldTable(String[] fieldTable) {
    this.fieldTable = fieldTable;
  }

  public String[] getFieldStream() {
    return fieldStream;
  }

  public void setFieldStream(String[] fieldStream) {
    this.fieldStream = fieldStream;
  }

  public DorisQueryVisitor getDorisQueryVisitor() {
    return dorisQueryVisitor;
  }

  public void setDorisQueryVisitor(DorisQueryVisitor dorisQueryVisitor) {
    this.dorisQueryVisitor = dorisQueryVisitor;
  }



}
