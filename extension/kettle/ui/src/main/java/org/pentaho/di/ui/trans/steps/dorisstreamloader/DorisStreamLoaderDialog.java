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

package org.pentaho.di.ui.trans.steps.dorisstreamloader;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.SourceToTargetMapping;
import org.pentaho.di.core.annotations.PluginDialog;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.dorisstreamloader.DorisStreamLoaderMeta;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisDataType;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisJdbcConnectionOptions;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisJdbcConnectionProvider;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisQueryVisitor;
import org.pentaho.di.ui.core.dialog.EnterMappingDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.*;

/**
 * Dialog class for the MySQL bulk loader step.
 */
@PluginDialog(id = "DorisStreamLoaderStep", image = "BLKMYSQL.svg", pluginType = PluginDialog.PluginType.STEP,
        documentationUrl = "http://wiki.pentaho.com/display/EAI/MySQL+Bulk+Loader")
@InjectionSupported(localizationPrefix = "DorisKettleConnector.Injection.", groups = {"FIELDS"})
public class DorisStreamLoaderDialog extends BaseStepDialog implements StepDialogInterface {
    private static Class<?> PKG = DorisStreamLoaderDialog.class; // for i18n purposes, needed by Translator2!!

    private DorisStreamLoaderMeta input;

    private Label wlJdbcUrl;
    private TextVar wJdbcUrl;
    private FormData fdlJdbcUrl, fdJdbcUrl;

    private Label wlHttpUrl;
    private TextVar wHttpUrl;
    private FormData fdlHttpUrl, fdHttpUrl;

    private Label wlDatabaseName;
    private TextVar wDatabaseName;
    private FormData fdlDatabaseName, fdDatabaseName;

    private Label wlTableName;
    private TextVar wTableName;
    private FormData fdlTableName, fdTableName;

    private Label wlUser;
    private TextVar wUser;
    private FormData fdlUser, fdUser;

    private Label wlPassword;
    private TextVar wPassword;
    private FormData fdlPassword, fdPassword;

    private Label wlStreamLoadProp;
    private TextVar wStreamLoadProp;
    private FormData fdlStreamLoadProp, fdStreamLoadProp;

    private Label wlBufferFlushMaxRows;
    private TextVar wBufferFlushMaxRows;
    private FormData fdlBufferFlushMaxRows, fdBufferFlushMaxRows;

    private Label wlBufferFlushMaxBytes;
    private TextVar wBufferFlushMaxBytes;
    private FormData fdlBufferFlushMaxBytes, fdBufferFlushMaxBytes;


    private Label wlMaxRetries;
    private TextVar wMaxRetries;
    private FormData fdlMaxRetries, fdMaxRetries;

    private Label wlReturn;
    private TableView wReturn;
    private FormData fdlReturn, fdReturn;

    private Button wGetLU;
    private FormData fdGetLU;
    private Listener lsGetLU;

    private Button wDoMapping;
    private FormData fdDoMapping;

    private ColumnInfo[] ciReturn;
    private Map<String, Integer> inputFields;
    private List<ColumnInfo> tableFieldColumns = new ArrayList<ColumnInfo>();


    public DorisStreamLoaderDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        input = (DorisStreamLoaderMeta) in;
        inputFields = new HashMap<String, Integer>();
    }


    @Override
    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
        props.setLook(shell);
        setShellImage(shell, input);

        ModifyListener lsMod = new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent modifyEvent) {
                input.setChanged();
            }
        };

    FocusListener lsFocusLost = new FocusAdapter() {
      @Override
      public void focusLost(FocusEvent focusEvent) {
        setTableFieldCombo();
      }
    };
        changed = input.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;
        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.Shell.Title"));

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        // Stepname line
        wlStepname = new Label(shell, SWT.RIGHT);
        wlStepname.setText(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.Stepname.Label"));
        props.setLook(wlStepname);
        fdlStepname = new FormData();
        fdlStepname.left = new FormAttachment(0, 0);
        fdlStepname.right = new FormAttachment(middle, -margin);
        fdlStepname.top = new FormAttachment(0, margin);
        wlStepname.setLayoutData(fdlStepname);
        wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wStepname.setText(stepname);
        props.setLook(wStepname);
        wStepname.addModifyListener(lsMod);
        fdStepname = new FormData();
        fdStepname.left = new FormAttachment(middle, 0);
        fdStepname.top = new FormAttachment(0, margin);
        fdStepname.right = new FormAttachment(100, 0);
        wStepname.setLayoutData(fdStepname);

        //jdbc url
        wlJdbcUrl = new Label(shell, SWT.RIGHT);
        wlJdbcUrl.setText(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.JdbcUrl.Label"));
        props.setLook(wlJdbcUrl);
        fdlJdbcUrl = new FormData();
        fdlJdbcUrl.left = new FormAttachment(0, 0);
        fdlJdbcUrl.right = new FormAttachment(middle, -margin);
        fdlJdbcUrl.top = new FormAttachment(wStepname, margin * 2);
        wlJdbcUrl.setLayoutData(fdlJdbcUrl);

        wJdbcUrl = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wJdbcUrl);
        wJdbcUrl.addModifyListener(lsMod);
        wJdbcUrl.addFocusListener(lsFocusLost);
        fdJdbcUrl = new FormData();
        fdJdbcUrl.left = new FormAttachment(middle, 0);
        fdJdbcUrl.right = new FormAttachment(100, 0);
        fdJdbcUrl.top = new FormAttachment(wStepname, margin * 2);
        wJdbcUrl.setLayoutData(fdJdbcUrl);

        //http url ..
        wlHttpUrl = new Label(shell, SWT.RIGHT);
        wlHttpUrl.setText(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.HttpUrl.Label"));
        props.setLook(wlHttpUrl);
        fdlHttpUrl = new FormData();
        fdlHttpUrl.left = new FormAttachment(0, 0);
        fdlHttpUrl.right = new FormAttachment(middle, -margin);
        fdlHttpUrl.top = new FormAttachment(wJdbcUrl, margin * 2);
        wlHttpUrl.setLayoutData(fdlHttpUrl);

        wHttpUrl = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wHttpUrl);
        wHttpUrl.addModifyListener(lsMod);
        wHttpUrl.addFocusListener(lsFocusLost);
        fdHttpUrl = new FormData();
        fdHttpUrl.left = new FormAttachment(middle, 0);
        fdHttpUrl.right = new FormAttachment(100, 0);
        fdHttpUrl.top = new FormAttachment(wJdbcUrl, margin * 2);
        wHttpUrl.setLayoutData(fdHttpUrl);

        // DataBase Name line...
        wlDatabaseName = new Label(shell, SWT.RIGHT);
        wlDatabaseName.setText(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.DatabaseName.Label"));
        props.setLook(wlDatabaseName);
        fdlDatabaseName = new FormData();
        fdlDatabaseName.left = new FormAttachment(0, 0);
        fdlDatabaseName.right = new FormAttachment(middle, -margin);
        fdlDatabaseName.top = new FormAttachment(wHttpUrl, margin * 2);
        wlDatabaseName.setLayoutData(fdlDatabaseName);

        wDatabaseName = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wDatabaseName);
        wDatabaseName.addModifyListener(lsMod);
        wDatabaseName.addFocusListener(lsFocusLost);
        fdDatabaseName = new FormData();
        fdDatabaseName.left = new FormAttachment(middle, 0);
        fdDatabaseName.right = new FormAttachment(100, 0);
        fdDatabaseName.top = new FormAttachment(wHttpUrl, margin * 2);
        wDatabaseName.setLayoutData(fdDatabaseName);


        // Table Name line...
        wlTableName = new Label(shell, SWT.RIGHT);
        wlTableName.setText(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.TableName.Label"));
        props.setLook(wlTableName);
        fdlTableName = new FormData();
        fdlTableName.left = new FormAttachment(0, 0);
        fdlTableName.right = new FormAttachment(middle, -margin);
        fdlTableName.top = new FormAttachment(wDatabaseName, margin * 2);
        wlTableName.setLayoutData(fdlTableName);

        wTableName = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wTableName);
        wTableName.addModifyListener(lsMod);
        wTableName.addFocusListener(lsFocusLost);
        //wTableName.setText(input.getTable());
        fdTableName = new FormData();
        fdTableName.left = new FormAttachment(middle, 0);
        fdTableName.right = new FormAttachment(100, 0);
        fdTableName.top = new FormAttachment(wDatabaseName, margin * 2);
        wTableName.setLayoutData(fdTableName);

        // User line...
        wlUser = new Label(shell, SWT.RIGHT);
        wlUser.setText(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.User.Label"));
        props.setLook(wlUser);
        fdlUser = new FormData();
        fdlUser.left = new FormAttachment(0, 0);
        fdlUser.right = new FormAttachment(middle, -margin);
        fdlUser.top = new FormAttachment(wTableName, margin * 2);
        wlUser.setLayoutData(fdlUser);

        wUser = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wUser);
        wUser.addModifyListener(lsMod);
        wUser.addFocusListener(lsFocusLost);
        fdUser = new FormData();
        fdUser.left = new FormAttachment(middle, 0);
        fdUser.right = new FormAttachment(100, 0);
        fdUser.top = new FormAttachment(wTableName, margin * 2);
        wUser.setLayoutData(fdUser);

        // Password line ...
        wlPassword = new Label(shell, SWT.RIGHT);
        wlPassword.setText(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.Password.Label"));
        props.setLook(wlPassword);
        fdlPassword = new FormData();
        fdlPassword.left = new FormAttachment(0, 0);
        fdlPassword.right = new FormAttachment(middle, -margin);
        fdlPassword.top = new FormAttachment(wUser, margin * 2);
        wlPassword.setLayoutData(fdlPassword);

        wPassword = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wPassword);
        wPassword.addModifyListener(lsMod);
        wPassword.addFocusListener(lsFocusLost);
        fdPassword = new FormData();
        fdPassword.left = new FormAttachment(middle, 0);
        fdPassword.right = new FormAttachment(100, 0);
        fdPassword.top = new FormAttachment(wUser, margin * 2);
        wPassword.setLayoutData(fdPassword);

        //streamLoadProp line ...
        wlStreamLoadProp = new Label(shell, SWT.RIGHT);
        wlStreamLoadProp.setText(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.StreamLoadProp.Label"));
        props.setLook(wlStreamLoadProp);
        fdlStreamLoadProp = new FormData();
        fdlStreamLoadProp.left = new FormAttachment(0, 0);
        fdlStreamLoadProp.right = new FormAttachment(middle, -margin);
        fdlStreamLoadProp.top = new FormAttachment(wPassword, margin * 2);
        wlStreamLoadProp.setLayoutData(fdlStreamLoadProp);

        wStreamLoadProp = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wStreamLoadProp);
        wStreamLoadProp.addModifyListener(lsMod);
        // wPassword.addFocusListener(lsFocusLost);
        fdStreamLoadProp = new FormData();
        fdStreamLoadProp.left = new FormAttachment(middle, 0);
        fdStreamLoadProp.right = new FormAttachment(100, 0);
        fdStreamLoadProp.top = new FormAttachment(wPassword, margin * 2);
        wStreamLoadProp.setLayoutData(fdStreamLoadProp);


        //bufferFlushMaxRows line ...
        wlBufferFlushMaxRows = new Label(shell, SWT.RIGHT);
        wlBufferFlushMaxRows.setText(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.BufferFlushMaxRows.Label"));
        props.setLook(wlBufferFlushMaxRows);
        fdlBufferFlushMaxRows = new FormData();
        fdlBufferFlushMaxRows.left = new FormAttachment(0, 0);
        fdlBufferFlushMaxRows.right = new FormAttachment(middle, -margin);
        fdlBufferFlushMaxRows.top = new FormAttachment(wStreamLoadProp, margin * 2);
        wlBufferFlushMaxRows.setLayoutData(fdlBufferFlushMaxRows);

        wBufferFlushMaxRows = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wBufferFlushMaxRows);
        wBufferFlushMaxRows.addModifyListener(lsMod);
        // wPassword.addFocusListener(lsFocusLost);
        fdBufferFlushMaxRows = new FormData();
        fdBufferFlushMaxRows.left = new FormAttachment(middle, 0);
        fdBufferFlushMaxRows.right = new FormAttachment(100, 0);
        fdBufferFlushMaxRows.top = new FormAttachment(wStreamLoadProp, margin * 2);
        wBufferFlushMaxRows.setLayoutData(fdBufferFlushMaxRows);

        //bufferFlushMaxBytes line ...
        wlBufferFlushMaxBytes = new Label(shell, SWT.RIGHT);
        wlBufferFlushMaxBytes.setText(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.BufferFlushMaxBytes.Label"));
        props.setLook(wlBufferFlushMaxBytes);
        fdlBufferFlushMaxBytes = new FormData();
        fdlBufferFlushMaxBytes.left = new FormAttachment(0, 0);
        fdlBufferFlushMaxBytes.right = new FormAttachment(middle, -margin);
        fdlBufferFlushMaxBytes.top = new FormAttachment(wBufferFlushMaxRows, margin * 2);
        wlBufferFlushMaxBytes.setLayoutData(fdlBufferFlushMaxBytes);

        wBufferFlushMaxBytes = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wBufferFlushMaxBytes);
        wBufferFlushMaxBytes.addModifyListener(lsMod);
        // wPassword.addFocusListener(lsFocusLost);
        fdBufferFlushMaxBytes = new FormData();
        fdBufferFlushMaxBytes.left = new FormAttachment(middle, 0);
        fdBufferFlushMaxBytes.right = new FormAttachment(100, 0);
        fdBufferFlushMaxBytes.top = new FormAttachment(wBufferFlushMaxRows, margin * 2);
        wBufferFlushMaxBytes.setLayoutData(fdBufferFlushMaxBytes);


        //maxRetries line ...
        wlMaxRetries = new Label(shell, SWT.RIGHT);
        wlMaxRetries.setText(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.MaxRetries.Label"));
        //wlMaxRetries.setText(BaseMessages.getString(PKG, "导入作业最大容错率"));
        props.setLook(wlMaxRetries);
        fdlMaxRetries = new FormData();
        fdlMaxRetries.left = new FormAttachment(0, 0);
        fdlMaxRetries.right = new FormAttachment(middle, -margin);
        fdlMaxRetries.top = new FormAttachment(wBufferFlushMaxBytes, margin * 2);
        wlMaxRetries.setLayoutData(fdlMaxRetries);

        wMaxRetries = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wMaxRetries);
        wMaxRetries.addModifyListener(lsMod);
        // wPassword.addFocusListener(lsFocusLost);
        fdMaxRetries = new FormData();
        fdMaxRetries.left = new FormAttachment(middle, 0);
        fdMaxRetries.right = new FormAttachment(100, 0);
        fdMaxRetries.top = new FormAttachment(wBufferFlushMaxBytes, margin * 2);
        wMaxRetries.setLayoutData(fdMaxRetries);


        // OK and cancel buttons
        wOK = new Button( shell, SWT.PUSH );
        wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
        wCancel = new Button( shell, SWT.PUSH );
        wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
        setButtonPositions(new Button[]{wOK, wCancel}, margin, null);

        // The field Table
        wlReturn = new Label(shell, SWT.NONE);
        wlReturn.setText(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.Fields.Label"));
        //wlReturn.setText(BaseMessages.getString(PKG, "要加载的字段"));
        props.setLook(wlReturn);
        fdlReturn = new FormData();
        fdlReturn.left = new FormAttachment(0, 0);
        fdlReturn.top = new FormAttachment(wMaxRetries, margin);
        wlReturn.setLayoutData(fdlReturn);

        int UpInsCols = 2;
        int UpInsRows = (input.getFieldTable() != null ? input.getFieldTable().length : 1);

        ciReturn = new ColumnInfo[UpInsCols];
        ciReturn[0] =
                new ColumnInfo(
                        BaseMessages.getString(PKG, "DorisKettleConnectorDialog.ColumnInfo.TableField"),
                        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[]{""}, false);
        ciReturn[1] =
                new ColumnInfo(
                        BaseMessages.getString(PKG, "DorisKettleConnectorDialog.ColumnInfo.StreamField"),
                        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[]{""}, false);

        tableFieldColumns.add(ciReturn[0]);
        wReturn =
                new TableView(
                        transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciReturn,
                        UpInsRows, lsMod, props);

        wGetLU = new Button(shell, SWT.PUSH);
        wGetLU.setText(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.GetFields.Label"));
        //wGetLU.setText(BaseMessages.getString(PKG, "获取字段"));
        fdGetLU = new FormData();
        fdGetLU.top = new FormAttachment(wlReturn, margin);
        fdGetLU.right = new FormAttachment(100, 0);
        wGetLU.setLayoutData(fdGetLU);

//        wGetLU.addListener(SWT.Selection, new Listener() {
//            @Override
//            public void handleEvent(Event event) {
//                setTableFieldCombo();
//            }
//        });

        wDoMapping = new Button(shell, SWT.PUSH);
        wDoMapping.setText(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.EditMapping.Label"));
        fdDoMapping = new FormData();
        fdDoMapping.top = new FormAttachment(wGetLU, margin);
        fdDoMapping.right = new FormAttachment(100, 0);
        wDoMapping.setLayoutData(fdDoMapping);

        wDoMapping.addListener(SWT.Selection, new Listener() {
            public void handleEvent(Event arg0) {
                generateMappings();
            }
        });

        fdReturn = new FormData();
        fdReturn.left = new FormAttachment(0, 0);
        fdReturn.top = new FormAttachment(wlReturn, margin);
        fdReturn.right = new FormAttachment(wDoMapping, -margin);
        fdReturn.bottom = new FormAttachment(wOK, -2 * margin);
        wReturn.setLayoutData(fdReturn);

        final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                StepMeta stepMeta = transMeta.findStep(stepname);
                if (stepMeta != null) {
                    try {
                        RowMetaInterface row = transMeta.getPrevStepFields(stepMeta);

                        // Remember these fields...
                        for (int i = 0; i < row.size(); i++) {
                            inputFields.put(row.getValueMeta(i).getName(), i);
                        }
                        setComboBoxes();
                    } catch (KettleException e) {
                        logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
                    }
                }
            }
        };
        new Thread(runnable).start();

        // Add listeners
        lsCancel = new Listener() {
            public void handleEvent( Event e ) {
                cancel();
            }
        };
        lsOK = new Listener() {
            public void handleEvent( Event e ) {
                ok();
            }
        };

        lsGetLU = new Listener() {
            @Override
            public void handleEvent(Event event) {
                getUpdate();
            }
        };

        wCancel.addListener( SWT.Selection, lsCancel );
        wOK.addListener( SWT.Selection, lsOK );
        wGetLU.addListener(SWT.Selection, lsGetLU);

        lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected( SelectionEvent e ) {
                ok();
            }
        };

        wJdbcUrl.addSelectionListener(lsDef);
        wHttpUrl.addSelectionListener(lsDef);
        wDatabaseName.addSelectionListener(lsDef);
        wTableName.addSelectionListener(lsDef);
        wUser.addSelectionListener(lsDef);
        wPassword.addSelectionListener(lsDef);
        wStreamLoadProp.addSelectionListener(lsDef);
        wBufferFlushMaxRows.addSelectionListener(lsDef);
        wBufferFlushMaxBytes.addSelectionListener(lsDef);
        wMaxRetries.addSelectionListener( lsDef );


        // Detect X or ALT-F4 or something that kills this window...
        shell.addShellListener( new ShellAdapter() {
            public void shellClosed( ShellEvent e ) {
                cancel();
            }
        } );

        // Set the shell size, based upon previous time...
        setSize();

        getData();
        setTableFieldCombo();
        input.setChanged( changed );

        shell.open();
        while ( !shell.isDisposed() ) {
            if ( !display.readAndDispatch() ) {
                display.sleep();
            }
        }

        return stepname;
    }

    private void getData(){
        if (log.isDebug()) {
            logDebug(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.Log.GettingKeyInfo"));
        }

        String streamLoadStr = String.valueOf(input.getStreamLoadProp());
        if (streamLoadStr.startsWith("{")){
            streamLoadStr= streamLoadStr.substring(1,streamLoadStr.length()-1);
        }
        if (streamLoadStr.endsWith("}")){
            streamLoadStr= streamLoadStr.substring(0,streamLoadStr.length()-2);
        }
        wStreamLoadProp.setText(Const.NVL(streamLoadStr,""));
        wBufferFlushMaxRows.setText(Const.NVL(String.valueOf(input.getBufferFlushMaxRows()),"10000"));
        wBufferFlushMaxBytes.setText(Const.NVL(String.valueOf(input.getBufferFlushMaxBytes()),"94371840"));
        wMaxRetries.setText(Const.NVL(String.valueOf(input.getMaxRetries()),"0"));

        if (input.getFieldTable() != null) {
            for (int i = 0; i < input.getFieldTable().length; i++) {
                TableItem item = wReturn.table.getItem(i);
                if (input.getFieldTable()[i] != null) {
                    item.setText(1, input.getFieldTable()[i]);
                }
                if (input.getFieldStream()[i] != null) {
                    item.setText(2, input.getFieldStream()[i]);
                }
            }
        }

        if (input.getJdbcUrl() != null){
            wJdbcUrl.setText(input.getJdbcUrl().toString());
        }
        if (input.getFenodes() != null){
            wHttpUrl.setText(input.getFenodes().toString());
        }
        if (input.getDatabase() != null) {
            wDatabaseName.setText(input.getDatabase().toString());
        }
        if (input.getTable() != null) {
            wTableName.setText(input.getTable().toString());
        }
        if (input.getUsername() != null) {
            wUser.setText(input.getUsername().toString());
        }
        if (input.getPassword() != null) {
            wPassword.setText(input.getPassword().toString());
        }

        wReturn.setRowNums();
        wReturn.optWidth(true);

        wStepname.selectAll();
        wStepname.setFocus();

    }
    private void cancel() {
        stepname = null;
        input.setChanged(changed);
        dispose();
    }

    // let the plugin know about the entered data
    private void ok() {
        if (Utils.isEmpty(wStepname.getText())) {
            return;
        }

        input.setChanged();
        getInfo(input);
        dispose();
    }

    private void generateMappings() {

        // Determine the source and target fields...
        //
        RowMetaInterface sourceFields;
        List<String> targetFields = new ArrayList<>();

        try {
            sourceFields = transMeta.getPrevStepFields(stepMeta);
        } catch (KettleException e) {
            new ErrorDialog(shell,
                    BaseMessages.getString(PKG, "DorisKettleConnectorDialog.DoMapping.UnableToFindSourceFields.Title"),
                    BaseMessages.getString(PKG, "DorisKettleConnectorDialog.DoMapping.UnableToFindSourceFields.Message"), e);
            return;
        }
        // refresh data
        input.setJdbcUrl(wJdbcUrl.getText());
        input.setFenodes(wHttpUrl.getText());
        input.setTable(wTableName.getText());
        input.setDatabase(wDatabaseName.getText());
        input.setUsername(wUser.getText());
        input.setPassword(wPassword.getText());


        String[] inputNames = new String[sourceFields.size()];
        for (int i = 0; i < sourceFields.size(); i++) {
            ValueMetaInterface value = sourceFields.getValueMeta(i);
            inputNames[i] = value.getName() + EnterMappingDialog.STRING_ORIGIN_SEPARATOR + value.getOrigin() + ")";
        }

        // Create the existing mapping list...
        //
        List<SourceToTargetMapping> mappings = new ArrayList<>();
        StringBuilder missingSourceFields = new StringBuilder();
        StringBuilder missingTargetFields = new StringBuilder();

        int nrFields = wReturn.nrNonEmpty();
        for (int i = 0; i < nrFields; i++) {
            TableItem item = wReturn.getNonEmpty(i);
            String source = item.getText(2);
            String target = item.getText(1);

            int sourceIndex = sourceFields.indexOfValue(source);
            if (sourceIndex < 0) {
                missingSourceFields.append(Const.CR).append("   ").append(source).append(" --> ").append(target);
            }
            int targetIndex = targetFields.indexOf(target);
            if (targetIndex < 0) {
                missingTargetFields.append(Const.CR).append("   ").append(source).append(" --> ").append(target);
            }
            if (sourceIndex < 0 || targetIndex < 0) {
                continue;
            }

            SourceToTargetMapping mapping = new SourceToTargetMapping(sourceIndex, targetIndex);
            mappings.add(mapping);
        }

        // show a confirm dialog if some missing field was found
        //
        if (missingSourceFields.length() > 0 || missingTargetFields.length() > 0) {

            String message = "";
            if (missingSourceFields.length() > 0) {
                message +=
                        BaseMessages.getString(
                                PKG, "DorisKettleConnectorDialog.DoMapping.SomeSourceFieldsNotFound", missingSourceFields.toString())
                                + Const.CR;
            }
            if (missingTargetFields.length() > 0) {
                message +=
                        BaseMessages.getString(
                                PKG, "DorisKettleConnectorDialog.DoMapping.SomeTargetFieldsNotFound", missingSourceFields.toString())
                                + Const.CR;
            }
            message += Const.CR;
            message +=
                    BaseMessages.getString(PKG, "DorisKettleConnectorDialog.DoMapping.SomeFieldsNotFoundContinue") + Const.CR;
            MessageDialog.setDefaultImage(GUIResource.getInstance().getImageSpoon());
            boolean goOn =
                    MessageDialog.openConfirm(shell, BaseMessages.getString(
                            PKG, "DorisKettleConnectorDialog.DoMapping.SomeFieldsNotFoundTitle"), message);
            if (!goOn) {
                return;
            }
        }
        EnterMappingDialog d = new EnterMappingDialog(DorisStreamLoaderDialog.this.shell, sourceFields.getFieldNames(),
                targetFields.toArray(new String[0]), mappings);
        mappings = d.open();

        // mappings == null if the user pressed cancel
        //
        if (mappings != null) {
            // Clear and re-populate!
            //
            wReturn.table.removeAll();
            wReturn.table.setItemCount(mappings.size());
            for (int i = 0; i < mappings.size(); i++) {
                SourceToTargetMapping mapping = mappings.get(i);
                TableItem item = wReturn.table.getItem(i);
                item.setText(2, sourceFields.getValueMeta(mapping.getSourcePosition()).getName());
                item.setText(1, targetFields.get(mapping.getTargetPosition()));
            }
            wReturn.setRowNums();
            wReturn.optWidth(true);
        }
    }

    private void getInfo(DorisStreamLoaderMeta inf) {
        int nrfields = wReturn.nrNonEmpty();

        inf.allocate(nrfields);


        if (log.isDebug()) {
            logDebug(BaseMessages.getString(PKG, "DorisKettleConnectorDialog.Log.FoundFields", "" + nrfields));
        }
        //CHECKSTYLE:Indentation:OFF
        for (int i = 0; i < nrfields; i++) {
            TableItem item = wReturn.getNonEmpty(i);
            inf.getFieldTable()[i] = item.getText(1);
            inf.getFieldStream()[i] = item.getText(2);
        }
        if (wHttpUrl.getText() != null && wHttpUrl.getText().length() != 0) {
            inf.setFenodes(wHttpUrl.getText());
        } else {
            inf.setFenodes(null);
        }

        input.setJdbcUrl(wJdbcUrl.getText());
        inf.setDatabase(wDatabaseName.getText());
        inf.setTable(wTableName.getText());
        inf.setUsername(wUser.getText());
        inf.setPassword(wPassword.getText());

        if (wStreamLoadProp.getText() != null && wStreamLoadProp.getText().length() != 0){
            Properties properties = new Properties();
            try {
                properties.load(new StringReader(wStreamLoadProp.getText()));
                inf.setStreamLoadProp(properties);
            }catch (IOException e){
                e.printStackTrace();
                new ErrorDialog(shell,
                        BaseMessages.getString(PKG, "DorisKettleConnectorDialog.StreamLoadProp.ParsingError.Title"),
                        BaseMessages.getString(PKG, "DorisKettleConnectorDialog.StreamLoadProp.ParsingError.Message"), e);
            }
        }else {
            inf.setStreamLoadProp(null);
        }

        inf.setBufferFlushMaxRows(Long.parseLong(wBufferFlushMaxRows.getText()));
        inf.setBufferFlushMaxBytes(Long.parseLong(wBufferFlushMaxBytes.getText()));
        inf.setMaxRetries(Integer.parseInt(wMaxRetries.getText()));




        stepname = wStepname.getText();

    }

    protected void setComboBoxes() {
        // Something was changed in the row.
        //
        final Map<String, Integer> fields = new HashMap<String, Integer>();

        // Add the currentMeta fields...
        fields.putAll(inputFields);

        Set<String> keySet = fields.keySet();
        List<String> entries = new ArrayList<String>(keySet);

        String[] fieldNames = entries.toArray(new String[entries.size()]);
        Const.sortStrings(fieldNames);
        // return fields
        ciReturn[1].setComboValues(fieldNames);
    }

    private void getUpdate() {
        try {
            RowMetaInterface r = transMeta.getPrevStepFields(stepname);
            if (r != null) {
                BaseStepDialog.getFieldsFromPrevious(r, wReturn, 1, new int[]{1, 2}, new int[]{}, -1, -1, null);
            }
        } catch (KettleException ke) {
            new ErrorDialog(
                    shell, BaseMessages.getString(PKG, "DorisKettleConnectorDialog.FailedToGetFields.DialogTitle"),
                    BaseMessages.getString(PKG, "DorisKettleConnectorDialog.FailedToGetFields.DialogMessage"), ke);
        }
    }
    private void setTableFieldCombo() {
        Runnable fieldLoader = new Runnable() {
            @Override
            public void run() {
                if (!wJdbcUrl.isDisposed() && !wTableName.isDisposed() && !wDatabaseName.isDisposed() && !wUser.isDisposed() && !wPassword.isDisposed()) {
                    final String jdbcUrl = wJdbcUrl.getText(), tableName = wTableName.getText(), databaseName = wDatabaseName.getText(), user = wUser.getText(), password = wPassword.getText();

                    // Clear
                    for (ColumnInfo colInfo : tableFieldColumns) {
                        colInfo.setComboValues(new String[]{});
                    }
                    if (!Utils.isEmpty(tableName) && !Utils.isEmpty(jdbcUrl) && !Utils.isEmpty(user)) {
                        try {
                            DorisJdbcConnectionOptions jdbcConnectionOptions = new DorisJdbcConnectionOptions(jdbcUrl, user, password);
                            DorisJdbcConnectionProvider jdbcConnectionProvider = new DorisJdbcConnectionProvider(jdbcConnectionOptions);
                            DorisQueryVisitor dorisQueryVisitor = new DorisQueryVisitor(jdbcConnectionProvider, databaseName, tableName);

                            Map<String, DorisDataType> fieldMap = dorisQueryVisitor.getFieldMapping();
                            if (null != fieldMap) {
                                String[] fieldNames = fieldMap.keySet().toArray(new String[0]);
                                for (ColumnInfo colInfo : tableFieldColumns) {
                                    colInfo.setComboValues(fieldNames);
                                }

                            }
                        } catch (Exception e) {
                            for (ColumnInfo colInfo : tableFieldColumns) {
                                colInfo.setComboValues(new String[]{});
                            }
                        }
                    }
                }
            }
        };
        shell.getDisplay().asyncExec(fieldLoader);
    }
}
