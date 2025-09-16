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
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.List;

/**
 * DorisStreamLoaderMeta
 */
@Step( id = "DorisStreamLoaderStep", name = "BaseStep.TypeLongDesc.DorisStreamLoader",
  description = "BaseStep.TypeTooltipDesc.DorisStreamLoader",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Bulk",
  image = "doris.svg",
  documentationUrl = "https://doris.apache.org/docs/dev/ecosystem/kettle/",
  i18nPackageName = "org.pentaho.di.trans.steps.dorisstreamloader" )
@InjectionSupported( localizationPrefix = "DorisStreamLoader.Injection.", groups = { "FIELDS" } )
public class DorisStreamLoaderMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = DorisStreamLoaderMeta.class; // for i18n purposes, needed by Translator2!!

  /** what's the schema for the target? */
  @Injection( name = "FENODES" )
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

  private String streamLoadProp;

  private long bufferFlushMaxRows;

  private long bufferFlushMaxBytes;

  private int maxRetries;

  private boolean deletable;

  /** Field name of the target table */
  @Injection( name = "FIELD_TABLE", group = "FIELDS" )
  private String[] fieldTable;

  /** Field name in the stream */
  @Injection( name = "FIELD_STREAM", group = "FIELDS" )
  private String[] fieldStream;


  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode, databases );
  }

  private void readData( Node stepnode, List<? extends SharedObjectInterface> databases ) throws KettleXMLException {
    try {
      fenodes = XMLHandler.getTagValue(stepnode, "fenodes");
      database = XMLHandler.getTagValue(stepnode, "database");
      table = XMLHandler.getTagValue(stepnode, "table");
      username = XMLHandler.getTagValue(stepnode, "username");
      password = XMLHandler.getTagValue(stepnode, "password");
      if (password == null) {
        password = "";
      }

      bufferFlushMaxRows = Long.valueOf(XMLHandler.getTagValue(stepnode, "bufferFlushMaxRows"));
      bufferFlushMaxBytes = Long.valueOf(XMLHandler.getTagValue(stepnode, "bufferFlushMaxBytes"));
      maxRetries = Integer.valueOf(XMLHandler.getTagValue(stepnode, "maxRetries"));
      streamLoadProp = XMLHandler.getTagValue(stepnode, "streamLoadProp");
      deletable = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "deletable"));

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
    fenodes = null;
    database = "";
    table = BaseMessages.getString(PKG, "DorisStreamLoaderMeta.DefaultTableName");
    username = "root";
    password = "";

    bufferFlushMaxRows = 10000;
    bufferFlushMaxBytes = 10 * 1024 * 1024;
    maxRetries = 3;
    streamLoadProp = "format:json;read_json_by_line:true";
    deletable = false;
    allocate(0);
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder(300);

    retval.append("    ").append(XMLHandler.addTagValue("fenodes", fenodes));
    retval.append("    ").append(XMLHandler.addTagValue("database", database));
    retval.append("    ").append(XMLHandler.addTagValue("table", table));
    retval.append("    ").append(XMLHandler.addTagValue("username", username));
    retval.append("    ").append(XMLHandler.addTagValue("password", password));
    retval.append("    ").append(XMLHandler.addTagValue("bufferFlushMaxRows", bufferFlushMaxRows));
    retval.append("    ").append(XMLHandler.addTagValue("bufferFlushMaxBytes", bufferFlushMaxBytes));
    retval.append("    ").append(XMLHandler.addTagValue("maxRetries", maxRetries));
    retval.append("    ").append(XMLHandler.addTagValue("streamLoadProp", streamLoadProp));
    retval.append("    ").append(XMLHandler.addTagValue("deletable", deletable));

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
      fenodes = rep.getStepAttributeString(id_step, "fenodes");
      database = rep.getStepAttributeString(id_step, "database");
      table = rep.getStepAttributeString(id_step, "table");
      username = rep.getStepAttributeString(id_step, "username");
      password = rep.getStepAttributeString(id_step, "password");
      if (password == null) {
        password = "";
      }

      bufferFlushMaxRows = Long.valueOf(rep.getStepAttributeString(id_step, "bufferFlushMaxRows"));
      bufferFlushMaxBytes = Long.valueOf(rep.getStepAttributeString(id_step, "bufferFlushMaxBytes"));
      maxRetries = Integer.valueOf(rep.getStepAttributeString(id_step, "maxRetries"));

      streamLoadProp = rep.getStepAttributeString(id_step, "streamLoadProp");
      deletable = rep.getStepAttributeBoolean(id_step, "deletable");
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
      rep.saveStepAttribute(id_transformation, id_step, "fenodes", fenodes);
      rep.saveStepAttribute(id_transformation, id_step, "database", database);
      rep.saveStepAttribute(id_transformation, id_step, "table", table);
      rep.saveStepAttribute(id_transformation, id_step, "username", username);
      rep.saveStepAttribute(id_transformation, id_step, "password", password);
      rep.saveStepAttribute(id_transformation, id_step, "streamLoadProp", streamLoadProp);
      rep.saveStepAttribute(id_transformation, id_step, "bufferFlushMaxRows", bufferFlushMaxRows);
      rep.saveStepAttribute(id_transformation, id_step, "bufferFlushMaxBytes", bufferFlushMaxBytes);
      rep.saveStepAttribute(id_transformation, id_step, "maxRetries", maxRetries);
      rep.saveStepAttribute(id_transformation, id_step, "deletable", deletable);

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
    //todo: check parameters
  }


  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
      Trans trans ) {
    return new DorisStreamLoader( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new DorisStreamLoaderData();
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

  /**
   * key:value;key:value
   * @return
   */
  public String getStreamLoadProp() {
    return streamLoadProp;
  }

  public void setStreamLoadProp(String streamLoadProp) {
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

  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  public boolean isDeletable() {
      return deletable;
  }

  public void setDeletable(boolean deletable) {
      this.deletable = deletable;
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

  public void allocate(int nrvalues) {
    fieldTable = new String[nrvalues];
    fieldStream = new String[nrvalues];
  }

  @Override
  public String toString() {
    return "DorisStreamLoaderMeta{" +
            "fenodes='" + fenodes + '\'' +
            ", database='" + database + '\'' +
            ", table='" + table + '\'' +
            ", username='" + username + '\'' +
            ", password='" + password + '\'' +
            ", streamLoadProp=" + streamLoadProp +
            ", bufferFlushMaxRows=" + bufferFlushMaxRows +
            ", bufferFlushMaxBytes=" + bufferFlushMaxBytes +
            ", maxRetries=" + maxRetries +
            ", deletable=" + deletable +
            ", fieldTable=" + Arrays.toString(fieldTable) +
            ", fieldStream=" + Arrays.toString(fieldStream) +
            '}';
  }

  /**
   * If we use injection we can have different arrays lengths.
   * We need synchronize them for consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
      int nrFields = (fieldTable == null) ? -1 : fieldTable.length;
      if (nrFields <= 0) {
          return;
      }
      String[][] rtnStrings = Utils.normalizeArrays(nrFields, fieldStream);
      fieldStream = rtnStrings[0];
  }
}
