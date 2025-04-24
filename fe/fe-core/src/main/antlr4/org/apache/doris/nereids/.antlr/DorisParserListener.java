// Generated from /mnt/disk1/sunchenyang/doris/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisParser.g4 by ANTLR 4.13.1
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link DorisParser}.
 */
public interface DorisParserListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link DorisParser#multiStatements}.
	 * @param ctx the parse tree
	 */
	void enterMultiStatements(DorisParser.MultiStatementsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#multiStatements}.
	 * @param ctx the parse tree
	 */
	void exitMultiStatements(DorisParser.MultiStatementsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void enterSingleStatement(DorisParser.SingleStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void exitSingleStatement(DorisParser.SingleStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementBaseAlias}
	 * labeled alternative in {@link DorisParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementBaseAlias(DorisParser.StatementBaseAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementBaseAlias}
	 * labeled alternative in {@link DorisParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementBaseAlias(DorisParser.StatementBaseAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code callProcedure}
	 * labeled alternative in {@link DorisParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCallProcedure(DorisParser.CallProcedureContext ctx);
	/**
	 * Exit a parse tree produced by the {@code callProcedure}
	 * labeled alternative in {@link DorisParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCallProcedure(DorisParser.CallProcedureContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createProcedure}
	 * labeled alternative in {@link DorisParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateProcedure(DorisParser.CreateProcedureContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createProcedure}
	 * labeled alternative in {@link DorisParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateProcedure(DorisParser.CreateProcedureContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropProcedure}
	 * labeled alternative in {@link DorisParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDropProcedure(DorisParser.DropProcedureContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropProcedure}
	 * labeled alternative in {@link DorisParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDropProcedure(DorisParser.DropProcedureContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showProcedureStatus}
	 * labeled alternative in {@link DorisParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowProcedureStatus(DorisParser.ShowProcedureStatusContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showProcedureStatus}
	 * labeled alternative in {@link DorisParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowProcedureStatus(DorisParser.ShowProcedureStatusContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCreateProcedure}
	 * labeled alternative in {@link DorisParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowCreateProcedure(DorisParser.ShowCreateProcedureContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCreateProcedure}
	 * labeled alternative in {@link DorisParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowCreateProcedure(DorisParser.ShowCreateProcedureContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showConfig}
	 * labeled alternative in {@link DorisParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowConfig(DorisParser.ShowConfigContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showConfig}
	 * labeled alternative in {@link DorisParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowConfig(DorisParser.ShowConfigContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterStatementDefault(DorisParser.StatementDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitStatementDefault(DorisParser.StatementDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedDmlStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedDmlStatementAlias(DorisParser.SupportedDmlStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedDmlStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedDmlStatementAlias(DorisParser.SupportedDmlStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedCreateStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedCreateStatementAlias(DorisParser.SupportedCreateStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedCreateStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedCreateStatementAlias(DorisParser.SupportedCreateStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedAlterStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedAlterStatementAlias(DorisParser.SupportedAlterStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedAlterStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedAlterStatementAlias(DorisParser.SupportedAlterStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code materializedViewStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterMaterializedViewStatementAlias(DorisParser.MaterializedViewStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code materializedViewStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitMaterializedViewStatementAlias(DorisParser.MaterializedViewStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedJobStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedJobStatementAlias(DorisParser.SupportedJobStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedJobStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedJobStatementAlias(DorisParser.SupportedJobStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code constraintStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterConstraintStatementAlias(DorisParser.ConstraintStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code constraintStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitConstraintStatementAlias(DorisParser.ConstraintStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedCleanStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedCleanStatementAlias(DorisParser.SupportedCleanStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedCleanStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedCleanStatementAlias(DorisParser.SupportedCleanStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedDescribeStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedDescribeStatementAlias(DorisParser.SupportedDescribeStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedDescribeStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedDescribeStatementAlias(DorisParser.SupportedDescribeStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedDropStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedDropStatementAlias(DorisParser.SupportedDropStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedDropStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedDropStatementAlias(DorisParser.SupportedDropStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedSetStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedSetStatementAlias(DorisParser.SupportedSetStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedSetStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedSetStatementAlias(DorisParser.SupportedSetStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedUnsetStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedUnsetStatementAlias(DorisParser.SupportedUnsetStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedUnsetStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedUnsetStatementAlias(DorisParser.SupportedUnsetStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedRefreshStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedRefreshStatementAlias(DorisParser.SupportedRefreshStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedRefreshStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedRefreshStatementAlias(DorisParser.SupportedRefreshStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedShowStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedShowStatementAlias(DorisParser.SupportedShowStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedShowStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedShowStatementAlias(DorisParser.SupportedShowStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedLoadStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedLoadStatementAlias(DorisParser.SupportedLoadStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedLoadStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedLoadStatementAlias(DorisParser.SupportedLoadStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedCancelStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedCancelStatementAlias(DorisParser.SupportedCancelStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedCancelStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedCancelStatementAlias(DorisParser.SupportedCancelStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedRecoverStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedRecoverStatementAlias(DorisParser.SupportedRecoverStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedRecoverStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedRecoverStatementAlias(DorisParser.SupportedRecoverStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedAdminStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedAdminStatementAlias(DorisParser.SupportedAdminStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedAdminStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedAdminStatementAlias(DorisParser.SupportedAdminStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedUseStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedUseStatementAlias(DorisParser.SupportedUseStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedUseStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedUseStatementAlias(DorisParser.SupportedUseStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedOtherStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedOtherStatementAlias(DorisParser.SupportedOtherStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedOtherStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedOtherStatementAlias(DorisParser.SupportedOtherStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code supportedStatsStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterSupportedStatsStatementAlias(DorisParser.SupportedStatsStatementAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code supportedStatsStatementAlias}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitSupportedStatsStatementAlias(DorisParser.SupportedStatsStatementAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unsupported}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void enterUnsupported(DorisParser.UnsupportedContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unsupported}
	 * labeled alternative in {@link DorisParser#statementBase}.
	 * @param ctx the parse tree
	 */
	void exitUnsupported(DorisParser.UnsupportedContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#unsupportedStatement}.
	 * @param ctx the parse tree
	 */
	void enterUnsupportedStatement(DorisParser.UnsupportedStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#unsupportedStatement}.
	 * @param ctx the parse tree
	 */
	void exitUnsupportedStatement(DorisParser.UnsupportedStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createMTMV}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateMTMV(DorisParser.CreateMTMVContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createMTMV}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateMTMV(DorisParser.CreateMTMVContext ctx);
	/**
	 * Enter a parse tree produced by the {@code refreshMTMV}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void enterRefreshMTMV(DorisParser.RefreshMTMVContext ctx);
	/**
	 * Exit a parse tree produced by the {@code refreshMTMV}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void exitRefreshMTMV(DorisParser.RefreshMTMVContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterMTMV}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterMTMV(DorisParser.AlterMTMVContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterMTMV}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterMTMV(DorisParser.AlterMTMVContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropMTMV}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropMTMV(DorisParser.DropMTMVContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropMTMV}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropMTMV(DorisParser.DropMTMVContext ctx);
	/**
	 * Enter a parse tree produced by the {@code pauseMTMV}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void enterPauseMTMV(DorisParser.PauseMTMVContext ctx);
	/**
	 * Exit a parse tree produced by the {@code pauseMTMV}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void exitPauseMTMV(DorisParser.PauseMTMVContext ctx);
	/**
	 * Enter a parse tree produced by the {@code resumeMTMV}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void enterResumeMTMV(DorisParser.ResumeMTMVContext ctx);
	/**
	 * Exit a parse tree produced by the {@code resumeMTMV}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void exitResumeMTMV(DorisParser.ResumeMTMVContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cancelMTMVTask}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void enterCancelMTMVTask(DorisParser.CancelMTMVTaskContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cancelMTMVTask}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void exitCancelMTMVTask(DorisParser.CancelMTMVTaskContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCreateMTMV}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCreateMTMV(DorisParser.ShowCreateMTMVContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCreateMTMV}
	 * labeled alternative in {@link DorisParser#materializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCreateMTMV(DorisParser.ShowCreateMTMVContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createScheduledJob}
	 * labeled alternative in {@link DorisParser#supportedJobStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateScheduledJob(DorisParser.CreateScheduledJobContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createScheduledJob}
	 * labeled alternative in {@link DorisParser#supportedJobStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateScheduledJob(DorisParser.CreateScheduledJobContext ctx);
	/**
	 * Enter a parse tree produced by the {@code pauseJob}
	 * labeled alternative in {@link DorisParser#supportedJobStatement}.
	 * @param ctx the parse tree
	 */
	void enterPauseJob(DorisParser.PauseJobContext ctx);
	/**
	 * Exit a parse tree produced by the {@code pauseJob}
	 * labeled alternative in {@link DorisParser#supportedJobStatement}.
	 * @param ctx the parse tree
	 */
	void exitPauseJob(DorisParser.PauseJobContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropJob}
	 * labeled alternative in {@link DorisParser#supportedJobStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropJob(DorisParser.DropJobContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropJob}
	 * labeled alternative in {@link DorisParser#supportedJobStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropJob(DorisParser.DropJobContext ctx);
	/**
	 * Enter a parse tree produced by the {@code resumeJob}
	 * labeled alternative in {@link DorisParser#supportedJobStatement}.
	 * @param ctx the parse tree
	 */
	void enterResumeJob(DorisParser.ResumeJobContext ctx);
	/**
	 * Exit a parse tree produced by the {@code resumeJob}
	 * labeled alternative in {@link DorisParser#supportedJobStatement}.
	 * @param ctx the parse tree
	 */
	void exitResumeJob(DorisParser.ResumeJobContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cancelJobTask}
	 * labeled alternative in {@link DorisParser#supportedJobStatement}.
	 * @param ctx the parse tree
	 */
	void enterCancelJobTask(DorisParser.CancelJobTaskContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cancelJobTask}
	 * labeled alternative in {@link DorisParser#supportedJobStatement}.
	 * @param ctx the parse tree
	 */
	void exitCancelJobTask(DorisParser.CancelJobTaskContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addConstraint}
	 * labeled alternative in {@link DorisParser#constraintStatement}.
	 * @param ctx the parse tree
	 */
	void enterAddConstraint(DorisParser.AddConstraintContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addConstraint}
	 * labeled alternative in {@link DorisParser#constraintStatement}.
	 * @param ctx the parse tree
	 */
	void exitAddConstraint(DorisParser.AddConstraintContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropConstraint}
	 * labeled alternative in {@link DorisParser#constraintStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropConstraint(DorisParser.DropConstraintContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropConstraint}
	 * labeled alternative in {@link DorisParser#constraintStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropConstraint(DorisParser.DropConstraintContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showConstraint}
	 * labeled alternative in {@link DorisParser#constraintStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowConstraint(DorisParser.ShowConstraintContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showConstraint}
	 * labeled alternative in {@link DorisParser#constraintStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowConstraint(DorisParser.ShowConstraintContext ctx);
	/**
	 * Enter a parse tree produced by the {@code insertTable}
	 * labeled alternative in {@link DorisParser#supportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void enterInsertTable(DorisParser.InsertTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code insertTable}
	 * labeled alternative in {@link DorisParser#supportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void exitInsertTable(DorisParser.InsertTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code update}
	 * labeled alternative in {@link DorisParser#supportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void enterUpdate(DorisParser.UpdateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code update}
	 * labeled alternative in {@link DorisParser#supportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void exitUpdate(DorisParser.UpdateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code delete}
	 * labeled alternative in {@link DorisParser#supportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void enterDelete(DorisParser.DeleteContext ctx);
	/**
	 * Exit a parse tree produced by the {@code delete}
	 * labeled alternative in {@link DorisParser#supportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void exitDelete(DorisParser.DeleteContext ctx);
	/**
	 * Enter a parse tree produced by the {@code load}
	 * labeled alternative in {@link DorisParser#supportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void enterLoad(DorisParser.LoadContext ctx);
	/**
	 * Exit a parse tree produced by the {@code load}
	 * labeled alternative in {@link DorisParser#supportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void exitLoad(DorisParser.LoadContext ctx);
	/**
	 * Enter a parse tree produced by the {@code export}
	 * labeled alternative in {@link DorisParser#supportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void enterExport(DorisParser.ExportContext ctx);
	/**
	 * Exit a parse tree produced by the {@code export}
	 * labeled alternative in {@link DorisParser#supportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void exitExport(DorisParser.ExportContext ctx);
	/**
	 * Enter a parse tree produced by the {@code replay}
	 * labeled alternative in {@link DorisParser#supportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void enterReplay(DorisParser.ReplayContext ctx);
	/**
	 * Exit a parse tree produced by the {@code replay}
	 * labeled alternative in {@link DorisParser#supportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void exitReplay(DorisParser.ReplayContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createTable}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateTable(DorisParser.CreateTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createTable}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateTable(DorisParser.CreateTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createView}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateView(DorisParser.CreateViewContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createView}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateView(DorisParser.CreateViewContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createFile}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateFile(DorisParser.CreateFileContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createFile}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateFile(DorisParser.CreateFileContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createTableLike}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateTableLike(DorisParser.CreateTableLikeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createTableLike}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateTableLike(DorisParser.CreateTableLikeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createRole}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateRole(DorisParser.CreateRoleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createRole}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateRole(DorisParser.CreateRoleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createWorkloadGroup}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateWorkloadGroup(DorisParser.CreateWorkloadGroupContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createWorkloadGroup}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateWorkloadGroup(DorisParser.CreateWorkloadGroupContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createCatalog}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateCatalog(DorisParser.CreateCatalogContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createCatalog}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateCatalog(DorisParser.CreateCatalogContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createRowPolicy}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateRowPolicy(DorisParser.CreateRowPolicyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createRowPolicy}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateRowPolicy(DorisParser.CreateRowPolicyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createStoragePolicy}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateStoragePolicy(DorisParser.CreateStoragePolicyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createStoragePolicy}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateStoragePolicy(DorisParser.CreateStoragePolicyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code buildIndex}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterBuildIndex(DorisParser.BuildIndexContext ctx);
	/**
	 * Exit a parse tree produced by the {@code buildIndex}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitBuildIndex(DorisParser.BuildIndexContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createIndex}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateIndex(DorisParser.CreateIndexContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createIndex}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateIndex(DorisParser.CreateIndexContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createSqlBlockRule}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateSqlBlockRule(DorisParser.CreateSqlBlockRuleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createSqlBlockRule}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateSqlBlockRule(DorisParser.CreateSqlBlockRuleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createEncryptkey}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateEncryptkey(DorisParser.CreateEncryptkeyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createEncryptkey}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateEncryptkey(DorisParser.CreateEncryptkeyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createUserDefineFunction}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateUserDefineFunction(DorisParser.CreateUserDefineFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createUserDefineFunction}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateUserDefineFunction(DorisParser.CreateUserDefineFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createAliasFunction}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateAliasFunction(DorisParser.CreateAliasFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createAliasFunction}
	 * labeled alternative in {@link DorisParser#supportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateAliasFunction(DorisParser.CreateAliasFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterSystem}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterSystem(DorisParser.AlterSystemContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterSystem}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterSystem(DorisParser.AlterSystemContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterView}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterView(DorisParser.AlterViewContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterView}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterView(DorisParser.AlterViewContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterCatalogRename}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterCatalogRename(DorisParser.AlterCatalogRenameContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterCatalogRename}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterCatalogRename(DorisParser.AlterCatalogRenameContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterRole}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterRole(DorisParser.AlterRoleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterRole}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterRole(DorisParser.AlterRoleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterStorageVault}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterStorageVault(DorisParser.AlterStorageVaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterStorageVault}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterStorageVault(DorisParser.AlterStorageVaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterWorkloadGroup}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterWorkloadGroup(DorisParser.AlterWorkloadGroupContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterWorkloadGroup}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterWorkloadGroup(DorisParser.AlterWorkloadGroupContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterCatalogProperties}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterCatalogProperties(DorisParser.AlterCatalogPropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterCatalogProperties}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterCatalogProperties(DorisParser.AlterCatalogPropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterWorkloadPolicy}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterWorkloadPolicy(DorisParser.AlterWorkloadPolicyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterWorkloadPolicy}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterWorkloadPolicy(DorisParser.AlterWorkloadPolicyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterSqlBlockRule}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterSqlBlockRule(DorisParser.AlterSqlBlockRuleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterSqlBlockRule}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterSqlBlockRule(DorisParser.AlterSqlBlockRuleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterCatalogComment}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterCatalogComment(DorisParser.AlterCatalogCommentContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterCatalogComment}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterCatalogComment(DorisParser.AlterCatalogCommentContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterDatabaseRename}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterDatabaseRename(DorisParser.AlterDatabaseRenameContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterDatabaseRename}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterDatabaseRename(DorisParser.AlterDatabaseRenameContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterTable}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterTable(DorisParser.AlterTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterTable}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterTable(DorisParser.AlterTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterTableAddRollup}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterTableAddRollup(DorisParser.AlterTableAddRollupContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterTableAddRollup}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterTableAddRollup(DorisParser.AlterTableAddRollupContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterTableDropRollup}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterTableDropRollup(DorisParser.AlterTableDropRollupContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterTableDropRollup}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterTableDropRollup(DorisParser.AlterTableDropRollupContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterTableProperties}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterTableProperties(DorisParser.AlterTablePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterTableProperties}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterTableProperties(DorisParser.AlterTablePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterDatabaseSetQuota}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterDatabaseSetQuota(DorisParser.AlterDatabaseSetQuotaContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterDatabaseSetQuota}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterDatabaseSetQuota(DorisParser.AlterDatabaseSetQuotaContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterSystemRenameComputeGroup}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterSystemRenameComputeGroup(DorisParser.AlterSystemRenameComputeGroupContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterSystemRenameComputeGroup}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterSystemRenameComputeGroup(DorisParser.AlterSystemRenameComputeGroupContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterRepository}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterRepository(DorisParser.AlterRepositoryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterRepository}
	 * labeled alternative in {@link DorisParser#supportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterRepository(DorisParser.AlterRepositoryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropCatalogRecycleBin}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropCatalogRecycleBin(DorisParser.DropCatalogRecycleBinContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropCatalogRecycleBin}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropCatalogRecycleBin(DorisParser.DropCatalogRecycleBinContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropEncryptkey}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropEncryptkey(DorisParser.DropEncryptkeyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropEncryptkey}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropEncryptkey(DorisParser.DropEncryptkeyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropRole}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropRole(DorisParser.DropRoleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropRole}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropRole(DorisParser.DropRoleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropSqlBlockRule}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropSqlBlockRule(DorisParser.DropSqlBlockRuleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropSqlBlockRule}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropSqlBlockRule(DorisParser.DropSqlBlockRuleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropUser}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropUser(DorisParser.DropUserContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropUser}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropUser(DorisParser.DropUserContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropStoragePolicy}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropStoragePolicy(DorisParser.DropStoragePolicyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropStoragePolicy}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropStoragePolicy(DorisParser.DropStoragePolicyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropWorkloadGroup}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropWorkloadGroup(DorisParser.DropWorkloadGroupContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropWorkloadGroup}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropWorkloadGroup(DorisParser.DropWorkloadGroupContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropCatalog}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropCatalog(DorisParser.DropCatalogContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropCatalog}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropCatalog(DorisParser.DropCatalogContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropFile}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropFile(DorisParser.DropFileContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropFile}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropFile(DorisParser.DropFileContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropWorkloadPolicy}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropWorkloadPolicy(DorisParser.DropWorkloadPolicyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropWorkloadPolicy}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropWorkloadPolicy(DorisParser.DropWorkloadPolicyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropRepository}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropRepository(DorisParser.DropRepositoryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropRepository}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropRepository(DorisParser.DropRepositoryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropTable(DorisParser.DropTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropTable(DorisParser.DropTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropDatabase}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropDatabase(DorisParser.DropDatabaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropDatabase}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropDatabase(DorisParser.DropDatabaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropFunction}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropFunction(DorisParser.DropFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropFunction}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropFunction(DorisParser.DropFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropIndex}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropIndex(DorisParser.DropIndexContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropIndex}
	 * labeled alternative in {@link DorisParser#supportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropIndex(DorisParser.DropIndexContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showVariables}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowVariables(DorisParser.ShowVariablesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showVariables}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowVariables(DorisParser.ShowVariablesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showAuthors}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowAuthors(DorisParser.ShowAuthorsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showAuthors}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowAuthors(DorisParser.ShowAuthorsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCreateDatabase}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCreateDatabase(DorisParser.ShowCreateDatabaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCreateDatabase}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCreateDatabase(DorisParser.ShowCreateDatabaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showBroker}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowBroker(DorisParser.ShowBrokerContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showBroker}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowBroker(DorisParser.ShowBrokerContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showDynamicPartition}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowDynamicPartition(DorisParser.ShowDynamicPartitionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showDynamicPartition}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowDynamicPartition(DorisParser.ShowDynamicPartitionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showEvents}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowEvents(DorisParser.ShowEventsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showEvents}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowEvents(DorisParser.ShowEventsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showLastInsert}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowLastInsert(DorisParser.ShowLastInsertContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showLastInsert}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowLastInsert(DorisParser.ShowLastInsertContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCharset}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCharset(DorisParser.ShowCharsetContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCharset}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCharset(DorisParser.ShowCharsetContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showDelete}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowDelete(DorisParser.ShowDeleteContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showDelete}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowDelete(DorisParser.ShowDeleteContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showGrants}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowGrants(DorisParser.ShowGrantsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showGrants}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowGrants(DorisParser.ShowGrantsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showGrantsForUser}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowGrantsForUser(DorisParser.ShowGrantsForUserContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showGrantsForUser}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowGrantsForUser(DorisParser.ShowGrantsForUserContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showSyncJob}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowSyncJob(DorisParser.ShowSyncJobContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showSyncJob}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowSyncJob(DorisParser.ShowSyncJobContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showLoadProfile}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowLoadProfile(DorisParser.ShowLoadProfileContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showLoadProfile}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowLoadProfile(DorisParser.ShowLoadProfileContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCreateRepository}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCreateRepository(DorisParser.ShowCreateRepositoryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCreateRepository}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCreateRepository(DorisParser.ShowCreateRepositoryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showView}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowView(DorisParser.ShowViewContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showView}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowView(DorisParser.ShowViewContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showPlugins}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowPlugins(DorisParser.ShowPluginsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showPlugins}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowPlugins(DorisParser.ShowPluginsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showRepositories}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowRepositories(DorisParser.ShowRepositoriesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showRepositories}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowRepositories(DorisParser.ShowRepositoriesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showEncryptKeys}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowEncryptKeys(DorisParser.ShowEncryptKeysContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showEncryptKeys}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowEncryptKeys(DorisParser.ShowEncryptKeysContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCreateTable}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCreateTable(DorisParser.ShowCreateTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCreateTable}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCreateTable(DorisParser.ShowCreateTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showProcessList}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowProcessList(DorisParser.ShowProcessListContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showProcessList}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowProcessList(DorisParser.ShowProcessListContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showRoles}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowRoles(DorisParser.ShowRolesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showRoles}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowRoles(DorisParser.ShowRolesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showPartitionId}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowPartitionId(DorisParser.ShowPartitionIdContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showPartitionId}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowPartitionId(DorisParser.ShowPartitionIdContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showPrivileges}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowPrivileges(DorisParser.ShowPrivilegesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showPrivileges}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowPrivileges(DorisParser.ShowPrivilegesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showProc}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowProc(DorisParser.ShowProcContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showProc}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowProc(DorisParser.ShowProcContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showSmallFiles}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowSmallFiles(DorisParser.ShowSmallFilesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showSmallFiles}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowSmallFiles(DorisParser.ShowSmallFilesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showStorageEngines}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowStorageEngines(DorisParser.ShowStorageEnginesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showStorageEngines}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowStorageEngines(DorisParser.ShowStorageEnginesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCreateCatalog}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCreateCatalog(DorisParser.ShowCreateCatalogContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCreateCatalog}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCreateCatalog(DorisParser.ShowCreateCatalogContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCatalog}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCatalog(DorisParser.ShowCatalogContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCatalog}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCatalog(DorisParser.ShowCatalogContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCatalogs}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCatalogs(DorisParser.ShowCatalogsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCatalogs}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCatalogs(DorisParser.ShowCatalogsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showUserProperties}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowUserProperties(DorisParser.ShowUserPropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showUserProperties}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowUserProperties(DorisParser.ShowUserPropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showAllProperties}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowAllProperties(DorisParser.ShowAllPropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showAllProperties}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowAllProperties(DorisParser.ShowAllPropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCollation}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCollation(DorisParser.ShowCollationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCollation}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCollation(DorisParser.ShowCollationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showStoragePolicy}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowStoragePolicy(DorisParser.ShowStoragePolicyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showStoragePolicy}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowStoragePolicy(DorisParser.ShowStoragePolicyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showSqlBlockRule}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowSqlBlockRule(DorisParser.ShowSqlBlockRuleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showSqlBlockRule}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowSqlBlockRule(DorisParser.ShowSqlBlockRuleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCreateView}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCreateView(DorisParser.ShowCreateViewContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCreateView}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCreateView(DorisParser.ShowCreateViewContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showDataTypes}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowDataTypes(DorisParser.ShowDataTypesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showDataTypes}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowDataTypes(DorisParser.ShowDataTypesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showData}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowData(DorisParser.ShowDataContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showData}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowData(DorisParser.ShowDataContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCreateMaterializedView}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCreateMaterializedView(DorisParser.ShowCreateMaterializedViewContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCreateMaterializedView}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCreateMaterializedView(DorisParser.ShowCreateMaterializedViewContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showWarningErrors}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowWarningErrors(DorisParser.ShowWarningErrorsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showWarningErrors}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowWarningErrors(DorisParser.ShowWarningErrorsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showWarningErrorCount}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowWarningErrorCount(DorisParser.ShowWarningErrorCountContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showWarningErrorCount}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowWarningErrorCount(DorisParser.ShowWarningErrorCountContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showBackends}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowBackends(DorisParser.ShowBackendsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showBackends}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowBackends(DorisParser.ShowBackendsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showStages}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowStages(DorisParser.ShowStagesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showStages}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowStages(DorisParser.ShowStagesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showReplicaDistribution}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowReplicaDistribution(DorisParser.ShowReplicaDistributionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showReplicaDistribution}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowReplicaDistribution(DorisParser.ShowReplicaDistributionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTriggers}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTriggers(DorisParser.ShowTriggersContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTriggers}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTriggers(DorisParser.ShowTriggersContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showDiagnoseTablet}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowDiagnoseTablet(DorisParser.ShowDiagnoseTabletContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showDiagnoseTablet}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowDiagnoseTablet(DorisParser.ShowDiagnoseTabletContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showFrontends}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowFrontends(DorisParser.ShowFrontendsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showFrontends}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowFrontends(DorisParser.ShowFrontendsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showDatabaseId}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowDatabaseId(DorisParser.ShowDatabaseIdContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showDatabaseId}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowDatabaseId(DorisParser.ShowDatabaseIdContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTableId}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTableId(DorisParser.ShowTableIdContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTableId}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTableId(DorisParser.ShowTableIdContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTrash}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTrash(DorisParser.ShowTrashContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTrash}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTrash(DorisParser.ShowTrashContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showStatus}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowStatus(DorisParser.ShowStatusContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showStatus}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowStatus(DorisParser.ShowStatusContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showWhitelist}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowWhitelist(DorisParser.ShowWhitelistContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showWhitelist}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowWhitelist(DorisParser.ShowWhitelistContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTabletsBelong}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTabletsBelong(DorisParser.ShowTabletsBelongContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTabletsBelong}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTabletsBelong(DorisParser.ShowTabletsBelongContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showDataSkew}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowDataSkew(DorisParser.ShowDataSkewContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showDataSkew}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowDataSkew(DorisParser.ShowDataSkewContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTableCreation}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTableCreation(DorisParser.ShowTableCreationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTableCreation}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTableCreation(DorisParser.ShowTableCreationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTabletStorageFormat}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTabletStorageFormat(DorisParser.ShowTabletStorageFormatContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTabletStorageFormat}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTabletStorageFormat(DorisParser.ShowTabletStorageFormatContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showQueryProfile}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowQueryProfile(DorisParser.ShowQueryProfileContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showQueryProfile}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowQueryProfile(DorisParser.ShowQueryProfileContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showConvertLsc}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowConvertLsc(DorisParser.ShowConvertLscContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showConvertLsc}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowConvertLsc(DorisParser.ShowConvertLscContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTables}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTables(DorisParser.ShowTablesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTables}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTables(DorisParser.ShowTablesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTableStatus}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTableStatus(DorisParser.ShowTableStatusContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTableStatus}
	 * labeled alternative in {@link DorisParser#supportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTableStatus(DorisParser.ShowTableStatusContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sync}
	 * labeled alternative in {@link DorisParser#supportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterSync(DorisParser.SyncContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sync}
	 * labeled alternative in {@link DorisParser#supportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitSync(DorisParser.SyncContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createRoutineLoadAlias}
	 * labeled alternative in {@link DorisParser#supportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateRoutineLoadAlias(DorisParser.CreateRoutineLoadAliasContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createRoutineLoadAlias}
	 * labeled alternative in {@link DorisParser#supportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateRoutineLoadAlias(DorisParser.CreateRoutineLoadAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code help}
	 * labeled alternative in {@link DorisParser#supportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void enterHelp(DorisParser.HelpContext ctx);
	/**
	 * Exit a parse tree produced by the {@code help}
	 * labeled alternative in {@link DorisParser#supportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void exitHelp(DorisParser.HelpContext ctx);
	/**
	 * Enter a parse tree produced by the {@code installPlugin}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void enterInstallPlugin(DorisParser.InstallPluginContext ctx);
	/**
	 * Exit a parse tree produced by the {@code installPlugin}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void exitInstallPlugin(DorisParser.InstallPluginContext ctx);
	/**
	 * Enter a parse tree produced by the {@code uninstallPlugin}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void enterUninstallPlugin(DorisParser.UninstallPluginContext ctx);
	/**
	 * Exit a parse tree produced by the {@code uninstallPlugin}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void exitUninstallPlugin(DorisParser.UninstallPluginContext ctx);
	/**
	 * Enter a parse tree produced by the {@code lockTables}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void enterLockTables(DorisParser.LockTablesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code lockTables}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void exitLockTables(DorisParser.LockTablesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unlockTables}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void enterUnlockTables(DorisParser.UnlockTablesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unlockTables}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void exitUnlockTables(DorisParser.UnlockTablesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code warmUpCluster}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void enterWarmUpCluster(DorisParser.WarmUpClusterContext ctx);
	/**
	 * Exit a parse tree produced by the {@code warmUpCluster}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void exitWarmUpCluster(DorisParser.WarmUpClusterContext ctx);
	/**
	 * Enter a parse tree produced by the {@code backup}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void enterBackup(DorisParser.BackupContext ctx);
	/**
	 * Exit a parse tree produced by the {@code backup}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void exitBackup(DorisParser.BackupContext ctx);
	/**
	 * Enter a parse tree produced by the {@code restore}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void enterRestore(DorisParser.RestoreContext ctx);
	/**
	 * Exit a parse tree produced by the {@code restore}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void exitRestore(DorisParser.RestoreContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unsupportedStartTransaction}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void enterUnsupportedStartTransaction(DorisParser.UnsupportedStartTransactionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unsupportedStartTransaction}
	 * labeled alternative in {@link DorisParser#unsupportedOtherStatement}.
	 * @param ctx the parse tree
	 */
	void exitUnsupportedStartTransaction(DorisParser.UnsupportedStartTransactionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#warmUpItem}.
	 * @param ctx the parse tree
	 */
	void enterWarmUpItem(DorisParser.WarmUpItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#warmUpItem}.
	 * @param ctx the parse tree
	 */
	void exitWarmUpItem(DorisParser.WarmUpItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#lockTable}.
	 * @param ctx the parse tree
	 */
	void enterLockTable(DorisParser.LockTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#lockTable}.
	 * @param ctx the parse tree
	 */
	void exitLockTable(DorisParser.LockTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showRowPolicy}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowRowPolicy(DorisParser.ShowRowPolicyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showRowPolicy}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowRowPolicy(DorisParser.ShowRowPolicyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showStorageVault}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowStorageVault(DorisParser.ShowStorageVaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showStorageVault}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowStorageVault(DorisParser.ShowStorageVaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showOpenTables}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowOpenTables(DorisParser.ShowOpenTablesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showOpenTables}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowOpenTables(DorisParser.ShowOpenTablesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showViews}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowViews(DorisParser.ShowViewsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showViews}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowViews(DorisParser.ShowViewsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showMaterializedView}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowMaterializedView(DorisParser.ShowMaterializedViewContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showMaterializedView}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowMaterializedView(DorisParser.ShowMaterializedViewContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCreateFunction}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCreateFunction(DorisParser.ShowCreateFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCreateFunction}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCreateFunction(DorisParser.ShowCreateFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showDatabases}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowDatabases(DorisParser.ShowDatabasesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showDatabases}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowDatabases(DorisParser.ShowDatabasesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowColumns(DorisParser.ShowColumnsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowColumns(DorisParser.ShowColumnsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showLoadWarings}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowLoadWarings(DorisParser.ShowLoadWaringsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showLoadWarings}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowLoadWarings(DorisParser.ShowLoadWaringsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showLoad}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowLoad(DorisParser.ShowLoadContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showLoad}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowLoad(DorisParser.ShowLoadContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showExport}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowExport(DorisParser.ShowExportContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showExport}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowExport(DorisParser.ShowExportContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showAlterTable}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowAlterTable(DorisParser.ShowAlterTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showAlterTable}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowAlterTable(DorisParser.ShowAlterTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showPartitions}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowPartitions(DorisParser.ShowPartitionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showPartitions}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowPartitions(DorisParser.ShowPartitionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTabletId}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTabletId(DorisParser.ShowTabletIdContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTabletId}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTabletId(DorisParser.ShowTabletIdContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTabletsFromTable}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTabletsFromTable(DorisParser.ShowTabletsFromTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTabletsFromTable}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTabletsFromTable(DorisParser.ShowTabletsFromTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showBackup}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowBackup(DorisParser.ShowBackupContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showBackup}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowBackup(DorisParser.ShowBackupContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showRestore}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowRestore(DorisParser.ShowRestoreContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showRestore}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowRestore(DorisParser.ShowRestoreContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showResources}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowResources(DorisParser.ShowResourcesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showResources}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowResources(DorisParser.ShowResourcesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showWorkloadGroups}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowWorkloadGroups(DorisParser.ShowWorkloadGroupsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showWorkloadGroups}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowWorkloadGroups(DorisParser.ShowWorkloadGroupsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showSnapshot}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowSnapshot(DorisParser.ShowSnapshotContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showSnapshot}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowSnapshot(DorisParser.ShowSnapshotContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showFunctions}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowFunctions(DorisParser.ShowFunctionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showFunctions}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowFunctions(DorisParser.ShowFunctionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showGlobalFunctions}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowGlobalFunctions(DorisParser.ShowGlobalFunctionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showGlobalFunctions}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowGlobalFunctions(DorisParser.ShowGlobalFunctionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTypeCast}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTypeCast(DorisParser.ShowTypeCastContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTypeCast}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTypeCast(DorisParser.ShowTypeCastContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showIndex}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowIndex(DorisParser.ShowIndexContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showIndex}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowIndex(DorisParser.ShowIndexContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTransaction}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTransaction(DorisParser.ShowTransactionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTransaction}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTransaction(DorisParser.ShowTransactionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCacheHotSpot}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCacheHotSpot(DorisParser.ShowCacheHotSpotContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCacheHotSpot}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCacheHotSpot(DorisParser.ShowCacheHotSpotContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCatalogRecycleBin}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCatalogRecycleBin(DorisParser.ShowCatalogRecycleBinContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCatalogRecycleBin}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCatalogRecycleBin(DorisParser.ShowCatalogRecycleBinContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showQueryStats}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowQueryStats(DorisParser.ShowQueryStatsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showQueryStats}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowQueryStats(DorisParser.ShowQueryStatsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showBuildIndex}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowBuildIndex(DorisParser.ShowBuildIndexContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showBuildIndex}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowBuildIndex(DorisParser.ShowBuildIndexContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showClusters}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowClusters(DorisParser.ShowClustersContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showClusters}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowClusters(DorisParser.ShowClustersContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showReplicaStatus}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowReplicaStatus(DorisParser.ShowReplicaStatusContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showReplicaStatus}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowReplicaStatus(DorisParser.ShowReplicaStatusContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCopy}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCopy(DorisParser.ShowCopyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCopy}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCopy(DorisParser.ShowCopyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showWarmUpJob}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowWarmUpJob(DorisParser.ShowWarmUpJobContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showWarmUpJob}
	 * labeled alternative in {@link DorisParser#unsupportedShowStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowWarmUpJob(DorisParser.ShowWarmUpJobContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#createRoutineLoad}.
	 * @param ctx the parse tree
	 */
	void enterCreateRoutineLoad(DorisParser.CreateRoutineLoadContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#createRoutineLoad}.
	 * @param ctx the parse tree
	 */
	void exitCreateRoutineLoad(DorisParser.CreateRoutineLoadContext ctx);
	/**
	 * Enter a parse tree produced by the {@code mysqlLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterMysqlLoad(DorisParser.MysqlLoadContext ctx);
	/**
	 * Exit a parse tree produced by the {@code mysqlLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitMysqlLoad(DorisParser.MysqlLoadContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createDataSyncJob}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateDataSyncJob(DorisParser.CreateDataSyncJobContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createDataSyncJob}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateDataSyncJob(DorisParser.CreateDataSyncJobContext ctx);
	/**
	 * Enter a parse tree produced by the {@code stopDataSyncJob}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterStopDataSyncJob(DorisParser.StopDataSyncJobContext ctx);
	/**
	 * Exit a parse tree produced by the {@code stopDataSyncJob}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitStopDataSyncJob(DorisParser.StopDataSyncJobContext ctx);
	/**
	 * Enter a parse tree produced by the {@code resumeDataSyncJob}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterResumeDataSyncJob(DorisParser.ResumeDataSyncJobContext ctx);
	/**
	 * Exit a parse tree produced by the {@code resumeDataSyncJob}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitResumeDataSyncJob(DorisParser.ResumeDataSyncJobContext ctx);
	/**
	 * Enter a parse tree produced by the {@code pauseDataSyncJob}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterPauseDataSyncJob(DorisParser.PauseDataSyncJobContext ctx);
	/**
	 * Exit a parse tree produced by the {@code pauseDataSyncJob}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitPauseDataSyncJob(DorisParser.PauseDataSyncJobContext ctx);
	/**
	 * Enter a parse tree produced by the {@code pauseRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterPauseRoutineLoad(DorisParser.PauseRoutineLoadContext ctx);
	/**
	 * Exit a parse tree produced by the {@code pauseRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitPauseRoutineLoad(DorisParser.PauseRoutineLoadContext ctx);
	/**
	 * Enter a parse tree produced by the {@code pauseAllRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterPauseAllRoutineLoad(DorisParser.PauseAllRoutineLoadContext ctx);
	/**
	 * Exit a parse tree produced by the {@code pauseAllRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitPauseAllRoutineLoad(DorisParser.PauseAllRoutineLoadContext ctx);
	/**
	 * Enter a parse tree produced by the {@code resumeRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterResumeRoutineLoad(DorisParser.ResumeRoutineLoadContext ctx);
	/**
	 * Exit a parse tree produced by the {@code resumeRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitResumeRoutineLoad(DorisParser.ResumeRoutineLoadContext ctx);
	/**
	 * Enter a parse tree produced by the {@code resumeAllRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterResumeAllRoutineLoad(DorisParser.ResumeAllRoutineLoadContext ctx);
	/**
	 * Exit a parse tree produced by the {@code resumeAllRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitResumeAllRoutineLoad(DorisParser.ResumeAllRoutineLoadContext ctx);
	/**
	 * Enter a parse tree produced by the {@code stopRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterStopRoutineLoad(DorisParser.StopRoutineLoadContext ctx);
	/**
	 * Exit a parse tree produced by the {@code stopRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitStopRoutineLoad(DorisParser.StopRoutineLoadContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowRoutineLoad(DorisParser.ShowRoutineLoadContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowRoutineLoad(DorisParser.ShowRoutineLoadContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showRoutineLoadTask}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowRoutineLoadTask(DorisParser.ShowRoutineLoadTaskContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showRoutineLoadTask}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowRoutineLoadTask(DorisParser.ShowRoutineLoadTaskContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCreateRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCreateRoutineLoad(DorisParser.ShowCreateRoutineLoadContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCreateRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCreateRoutineLoad(DorisParser.ShowCreateRoutineLoadContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCreateLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCreateLoad(DorisParser.ShowCreateLoadContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCreateLoad}
	 * labeled alternative in {@link DorisParser#unsupportedLoadStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCreateLoad(DorisParser.ShowCreateLoadContext ctx);
	/**
	 * Enter a parse tree produced by the {@code separator}
	 * labeled alternative in {@link DorisParser#loadProperty}.
	 * @param ctx the parse tree
	 */
	void enterSeparator(DorisParser.SeparatorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code separator}
	 * labeled alternative in {@link DorisParser#loadProperty}.
	 * @param ctx the parse tree
	 */
	void exitSeparator(DorisParser.SeparatorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code importColumns}
	 * labeled alternative in {@link DorisParser#loadProperty}.
	 * @param ctx the parse tree
	 */
	void enterImportColumns(DorisParser.ImportColumnsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code importColumns}
	 * labeled alternative in {@link DorisParser#loadProperty}.
	 * @param ctx the parse tree
	 */
	void exitImportColumns(DorisParser.ImportColumnsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code importPrecedingFilter}
	 * labeled alternative in {@link DorisParser#loadProperty}.
	 * @param ctx the parse tree
	 */
	void enterImportPrecedingFilter(DorisParser.ImportPrecedingFilterContext ctx);
	/**
	 * Exit a parse tree produced by the {@code importPrecedingFilter}
	 * labeled alternative in {@link DorisParser#loadProperty}.
	 * @param ctx the parse tree
	 */
	void exitImportPrecedingFilter(DorisParser.ImportPrecedingFilterContext ctx);
	/**
	 * Enter a parse tree produced by the {@code importWhere}
	 * labeled alternative in {@link DorisParser#loadProperty}.
	 * @param ctx the parse tree
	 */
	void enterImportWhere(DorisParser.ImportWhereContext ctx);
	/**
	 * Exit a parse tree produced by the {@code importWhere}
	 * labeled alternative in {@link DorisParser#loadProperty}.
	 * @param ctx the parse tree
	 */
	void exitImportWhere(DorisParser.ImportWhereContext ctx);
	/**
	 * Enter a parse tree produced by the {@code importDeleteOn}
	 * labeled alternative in {@link DorisParser#loadProperty}.
	 * @param ctx the parse tree
	 */
	void enterImportDeleteOn(DorisParser.ImportDeleteOnContext ctx);
	/**
	 * Exit a parse tree produced by the {@code importDeleteOn}
	 * labeled alternative in {@link DorisParser#loadProperty}.
	 * @param ctx the parse tree
	 */
	void exitImportDeleteOn(DorisParser.ImportDeleteOnContext ctx);
	/**
	 * Enter a parse tree produced by the {@code importSequence}
	 * labeled alternative in {@link DorisParser#loadProperty}.
	 * @param ctx the parse tree
	 */
	void enterImportSequence(DorisParser.ImportSequenceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code importSequence}
	 * labeled alternative in {@link DorisParser#loadProperty}.
	 * @param ctx the parse tree
	 */
	void exitImportSequence(DorisParser.ImportSequenceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code importPartitions}
	 * labeled alternative in {@link DorisParser#loadProperty}.
	 * @param ctx the parse tree
	 */
	void enterImportPartitions(DorisParser.ImportPartitionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code importPartitions}
	 * labeled alternative in {@link DorisParser#loadProperty}.
	 * @param ctx the parse tree
	 */
	void exitImportPartitions(DorisParser.ImportPartitionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#importSequenceStatement}.
	 * @param ctx the parse tree
	 */
	void enterImportSequenceStatement(DorisParser.ImportSequenceStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#importSequenceStatement}.
	 * @param ctx the parse tree
	 */
	void exitImportSequenceStatement(DorisParser.ImportSequenceStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#importDeleteOnStatement}.
	 * @param ctx the parse tree
	 */
	void enterImportDeleteOnStatement(DorisParser.ImportDeleteOnStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#importDeleteOnStatement}.
	 * @param ctx the parse tree
	 */
	void exitImportDeleteOnStatement(DorisParser.ImportDeleteOnStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#importWhereStatement}.
	 * @param ctx the parse tree
	 */
	void enterImportWhereStatement(DorisParser.ImportWhereStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#importWhereStatement}.
	 * @param ctx the parse tree
	 */
	void exitImportWhereStatement(DorisParser.ImportWhereStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#importPrecedingFilterStatement}.
	 * @param ctx the parse tree
	 */
	void enterImportPrecedingFilterStatement(DorisParser.ImportPrecedingFilterStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#importPrecedingFilterStatement}.
	 * @param ctx the parse tree
	 */
	void exitImportPrecedingFilterStatement(DorisParser.ImportPrecedingFilterStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#importColumnsStatement}.
	 * @param ctx the parse tree
	 */
	void enterImportColumnsStatement(DorisParser.ImportColumnsStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#importColumnsStatement}.
	 * @param ctx the parse tree
	 */
	void exitImportColumnsStatement(DorisParser.ImportColumnsStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#importColumnDesc}.
	 * @param ctx the parse tree
	 */
	void enterImportColumnDesc(DorisParser.ImportColumnDescContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#importColumnDesc}.
	 * @param ctx the parse tree
	 */
	void exitImportColumnDesc(DorisParser.ImportColumnDescContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#channelDescriptions}.
	 * @param ctx the parse tree
	 */
	void enterChannelDescriptions(DorisParser.ChannelDescriptionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#channelDescriptions}.
	 * @param ctx the parse tree
	 */
	void exitChannelDescriptions(DorisParser.ChannelDescriptionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#channelDescription}.
	 * @param ctx the parse tree
	 */
	void enterChannelDescription(DorisParser.ChannelDescriptionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#channelDescription}.
	 * @param ctx the parse tree
	 */
	void exitChannelDescription(DorisParser.ChannelDescriptionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code refreshCatalog}
	 * labeled alternative in {@link DorisParser#supportedRefreshStatement}.
	 * @param ctx the parse tree
	 */
	void enterRefreshCatalog(DorisParser.RefreshCatalogContext ctx);
	/**
	 * Exit a parse tree produced by the {@code refreshCatalog}
	 * labeled alternative in {@link DorisParser#supportedRefreshStatement}.
	 * @param ctx the parse tree
	 */
	void exitRefreshCatalog(DorisParser.RefreshCatalogContext ctx);
	/**
	 * Enter a parse tree produced by the {@code refreshDatabase}
	 * labeled alternative in {@link DorisParser#supportedRefreshStatement}.
	 * @param ctx the parse tree
	 */
	void enterRefreshDatabase(DorisParser.RefreshDatabaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code refreshDatabase}
	 * labeled alternative in {@link DorisParser#supportedRefreshStatement}.
	 * @param ctx the parse tree
	 */
	void exitRefreshDatabase(DorisParser.RefreshDatabaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code refreshTable}
	 * labeled alternative in {@link DorisParser#supportedRefreshStatement}.
	 * @param ctx the parse tree
	 */
	void enterRefreshTable(DorisParser.RefreshTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code refreshTable}
	 * labeled alternative in {@link DorisParser#supportedRefreshStatement}.
	 * @param ctx the parse tree
	 */
	void exitRefreshTable(DorisParser.RefreshTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cleanAllProfile}
	 * labeled alternative in {@link DorisParser#supportedCleanStatement}.
	 * @param ctx the parse tree
	 */
	void enterCleanAllProfile(DorisParser.CleanAllProfileContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cleanAllProfile}
	 * labeled alternative in {@link DorisParser#supportedCleanStatement}.
	 * @param ctx the parse tree
	 */
	void exitCleanAllProfile(DorisParser.CleanAllProfileContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cleanLabel}
	 * labeled alternative in {@link DorisParser#supportedCleanStatement}.
	 * @param ctx the parse tree
	 */
	void enterCleanLabel(DorisParser.CleanLabelContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cleanLabel}
	 * labeled alternative in {@link DorisParser#supportedCleanStatement}.
	 * @param ctx the parse tree
	 */
	void exitCleanLabel(DorisParser.CleanLabelContext ctx);
	/**
	 * Enter a parse tree produced by the {@code refreshLdap}
	 * labeled alternative in {@link DorisParser#unsupportedRefreshStatement}.
	 * @param ctx the parse tree
	 */
	void enterRefreshLdap(DorisParser.RefreshLdapContext ctx);
	/**
	 * Exit a parse tree produced by the {@code refreshLdap}
	 * labeled alternative in {@link DorisParser#unsupportedRefreshStatement}.
	 * @param ctx the parse tree
	 */
	void exitRefreshLdap(DorisParser.RefreshLdapContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cleanQueryStats}
	 * labeled alternative in {@link DorisParser#unsupportedCleanStatement}.
	 * @param ctx the parse tree
	 */
	void enterCleanQueryStats(DorisParser.CleanQueryStatsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cleanQueryStats}
	 * labeled alternative in {@link DorisParser#unsupportedCleanStatement}.
	 * @param ctx the parse tree
	 */
	void exitCleanQueryStats(DorisParser.CleanQueryStatsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cleanAllQueryStats}
	 * labeled alternative in {@link DorisParser#unsupportedCleanStatement}.
	 * @param ctx the parse tree
	 */
	void enterCleanAllQueryStats(DorisParser.CleanAllQueryStatsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cleanAllQueryStats}
	 * labeled alternative in {@link DorisParser#unsupportedCleanStatement}.
	 * @param ctx the parse tree
	 */
	void exitCleanAllQueryStats(DorisParser.CleanAllQueryStatsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cancelLoad}
	 * labeled alternative in {@link DorisParser#supportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void enterCancelLoad(DorisParser.CancelLoadContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cancelLoad}
	 * labeled alternative in {@link DorisParser#supportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void exitCancelLoad(DorisParser.CancelLoadContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cancelExport}
	 * labeled alternative in {@link DorisParser#supportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void enterCancelExport(DorisParser.CancelExportContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cancelExport}
	 * labeled alternative in {@link DorisParser#supportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void exitCancelExport(DorisParser.CancelExportContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cancelWarmUpJob}
	 * labeled alternative in {@link DorisParser#supportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void enterCancelWarmUpJob(DorisParser.CancelWarmUpJobContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cancelWarmUpJob}
	 * labeled alternative in {@link DorisParser#supportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void exitCancelWarmUpJob(DorisParser.CancelWarmUpJobContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cancelAlterTable}
	 * labeled alternative in {@link DorisParser#unsupportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void enterCancelAlterTable(DorisParser.CancelAlterTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cancelAlterTable}
	 * labeled alternative in {@link DorisParser#unsupportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void exitCancelAlterTable(DorisParser.CancelAlterTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cancelBuildIndex}
	 * labeled alternative in {@link DorisParser#unsupportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void enterCancelBuildIndex(DorisParser.CancelBuildIndexContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cancelBuildIndex}
	 * labeled alternative in {@link DorisParser#unsupportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void exitCancelBuildIndex(DorisParser.CancelBuildIndexContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cancelDecommisionBackend}
	 * labeled alternative in {@link DorisParser#unsupportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void enterCancelDecommisionBackend(DorisParser.CancelDecommisionBackendContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cancelDecommisionBackend}
	 * labeled alternative in {@link DorisParser#unsupportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void exitCancelDecommisionBackend(DorisParser.CancelDecommisionBackendContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cancelBackup}
	 * labeled alternative in {@link DorisParser#unsupportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void enterCancelBackup(DorisParser.CancelBackupContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cancelBackup}
	 * labeled alternative in {@link DorisParser#unsupportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void exitCancelBackup(DorisParser.CancelBackupContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cancelRestore}
	 * labeled alternative in {@link DorisParser#unsupportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void enterCancelRestore(DorisParser.CancelRestoreContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cancelRestore}
	 * labeled alternative in {@link DorisParser#unsupportedCancelStatement}.
	 * @param ctx the parse tree
	 */
	void exitCancelRestore(DorisParser.CancelRestoreContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminShowReplicaDistribution}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminShowReplicaDistribution(DorisParser.AdminShowReplicaDistributionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminShowReplicaDistribution}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminShowReplicaDistribution(DorisParser.AdminShowReplicaDistributionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminRebalanceDisk}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminRebalanceDisk(DorisParser.AdminRebalanceDiskContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminRebalanceDisk}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminRebalanceDisk(DorisParser.AdminRebalanceDiskContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminCancelRebalanceDisk}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminCancelRebalanceDisk(DorisParser.AdminCancelRebalanceDiskContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminCancelRebalanceDisk}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminCancelRebalanceDisk(DorisParser.AdminCancelRebalanceDiskContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminDiagnoseTablet}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminDiagnoseTablet(DorisParser.AdminDiagnoseTabletContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminDiagnoseTablet}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminDiagnoseTablet(DorisParser.AdminDiagnoseTabletContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminShowReplicaStatus}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminShowReplicaStatus(DorisParser.AdminShowReplicaStatusContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminShowReplicaStatus}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminShowReplicaStatus(DorisParser.AdminShowReplicaStatusContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminCompactTable}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminCompactTable(DorisParser.AdminCompactTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminCompactTable}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminCompactTable(DorisParser.AdminCompactTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminCheckTablets}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminCheckTablets(DorisParser.AdminCheckTabletsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminCheckTablets}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminCheckTablets(DorisParser.AdminCheckTabletsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminShowTabletStorageFormat}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminShowTabletStorageFormat(DorisParser.AdminShowTabletStorageFormatContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminShowTabletStorageFormat}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminShowTabletStorageFormat(DorisParser.AdminShowTabletStorageFormatContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminCleanTrash}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminCleanTrash(DorisParser.AdminCleanTrashContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminCleanTrash}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminCleanTrash(DorisParser.AdminCleanTrashContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminSetTableStatus}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminSetTableStatus(DorisParser.AdminSetTableStatusContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminSetTableStatus}
	 * labeled alternative in {@link DorisParser#supportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminSetTableStatus(DorisParser.AdminSetTableStatusContext ctx);
	/**
	 * Enter a parse tree produced by the {@code recoverDatabase}
	 * labeled alternative in {@link DorisParser#supportedRecoverStatement}.
	 * @param ctx the parse tree
	 */
	void enterRecoverDatabase(DorisParser.RecoverDatabaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code recoverDatabase}
	 * labeled alternative in {@link DorisParser#supportedRecoverStatement}.
	 * @param ctx the parse tree
	 */
	void exitRecoverDatabase(DorisParser.RecoverDatabaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code recoverTable}
	 * labeled alternative in {@link DorisParser#supportedRecoverStatement}.
	 * @param ctx the parse tree
	 */
	void enterRecoverTable(DorisParser.RecoverTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code recoverTable}
	 * labeled alternative in {@link DorisParser#supportedRecoverStatement}.
	 * @param ctx the parse tree
	 */
	void exitRecoverTable(DorisParser.RecoverTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code recoverPartition}
	 * labeled alternative in {@link DorisParser#supportedRecoverStatement}.
	 * @param ctx the parse tree
	 */
	void enterRecoverPartition(DorisParser.RecoverPartitionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code recoverPartition}
	 * labeled alternative in {@link DorisParser#supportedRecoverStatement}.
	 * @param ctx the parse tree
	 */
	void exitRecoverPartition(DorisParser.RecoverPartitionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminSetReplicaStatus}
	 * labeled alternative in {@link DorisParser#unsupportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminSetReplicaStatus(DorisParser.AdminSetReplicaStatusContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminSetReplicaStatus}
	 * labeled alternative in {@link DorisParser#unsupportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminSetReplicaStatus(DorisParser.AdminSetReplicaStatusContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminSetReplicaVersion}
	 * labeled alternative in {@link DorisParser#unsupportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminSetReplicaVersion(DorisParser.AdminSetReplicaVersionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminSetReplicaVersion}
	 * labeled alternative in {@link DorisParser#unsupportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminSetReplicaVersion(DorisParser.AdminSetReplicaVersionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminRepairTable}
	 * labeled alternative in {@link DorisParser#unsupportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminRepairTable(DorisParser.AdminRepairTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminRepairTable}
	 * labeled alternative in {@link DorisParser#unsupportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminRepairTable(DorisParser.AdminRepairTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminCancelRepairTable}
	 * labeled alternative in {@link DorisParser#unsupportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminCancelRepairTable(DorisParser.AdminCancelRepairTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminCancelRepairTable}
	 * labeled alternative in {@link DorisParser#unsupportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminCancelRepairTable(DorisParser.AdminCancelRepairTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminSetFrontendConfig}
	 * labeled alternative in {@link DorisParser#unsupportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminSetFrontendConfig(DorisParser.AdminSetFrontendConfigContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminSetFrontendConfig}
	 * labeled alternative in {@link DorisParser#unsupportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminSetFrontendConfig(DorisParser.AdminSetFrontendConfigContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminSetPartitionVersion}
	 * labeled alternative in {@link DorisParser#unsupportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminSetPartitionVersion(DorisParser.AdminSetPartitionVersionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminSetPartitionVersion}
	 * labeled alternative in {@link DorisParser#unsupportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminSetPartitionVersion(DorisParser.AdminSetPartitionVersionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code adminCopyTablet}
	 * labeled alternative in {@link DorisParser#unsupportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void enterAdminCopyTablet(DorisParser.AdminCopyTabletContext ctx);
	/**
	 * Exit a parse tree produced by the {@code adminCopyTablet}
	 * labeled alternative in {@link DorisParser#unsupportedAdminStatement}.
	 * @param ctx the parse tree
	 */
	void exitAdminCopyTablet(DorisParser.AdminCopyTabletContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#baseTableRef}.
	 * @param ctx the parse tree
	 */
	void enterBaseTableRef(DorisParser.BaseTableRefContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#baseTableRef}.
	 * @param ctx the parse tree
	 */
	void exitBaseTableRef(DorisParser.BaseTableRefContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#wildWhere}.
	 * @param ctx the parse tree
	 */
	void enterWildWhere(DorisParser.WildWhereContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#wildWhere}.
	 * @param ctx the parse tree
	 */
	void exitWildWhere(DorisParser.WildWhereContext ctx);
	/**
	 * Enter a parse tree produced by the {@code transactionBegin}
	 * labeled alternative in {@link DorisParser#unsupportedTransactionStatement}.
	 * @param ctx the parse tree
	 */
	void enterTransactionBegin(DorisParser.TransactionBeginContext ctx);
	/**
	 * Exit a parse tree produced by the {@code transactionBegin}
	 * labeled alternative in {@link DorisParser#unsupportedTransactionStatement}.
	 * @param ctx the parse tree
	 */
	void exitTransactionBegin(DorisParser.TransactionBeginContext ctx);
	/**
	 * Enter a parse tree produced by the {@code transcationCommit}
	 * labeled alternative in {@link DorisParser#unsupportedTransactionStatement}.
	 * @param ctx the parse tree
	 */
	void enterTranscationCommit(DorisParser.TranscationCommitContext ctx);
	/**
	 * Exit a parse tree produced by the {@code transcationCommit}
	 * labeled alternative in {@link DorisParser#unsupportedTransactionStatement}.
	 * @param ctx the parse tree
	 */
	void exitTranscationCommit(DorisParser.TranscationCommitContext ctx);
	/**
	 * Enter a parse tree produced by the {@code transactionRollback}
	 * labeled alternative in {@link DorisParser#unsupportedTransactionStatement}.
	 * @param ctx the parse tree
	 */
	void enterTransactionRollback(DorisParser.TransactionRollbackContext ctx);
	/**
	 * Exit a parse tree produced by the {@code transactionRollback}
	 * labeled alternative in {@link DorisParser#unsupportedTransactionStatement}.
	 * @param ctx the parse tree
	 */
	void exitTransactionRollback(DorisParser.TransactionRollbackContext ctx);
	/**
	 * Enter a parse tree produced by the {@code grantTablePrivilege}
	 * labeled alternative in {@link DorisParser#unsupportedGrantRevokeStatement}.
	 * @param ctx the parse tree
	 */
	void enterGrantTablePrivilege(DorisParser.GrantTablePrivilegeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code grantTablePrivilege}
	 * labeled alternative in {@link DorisParser#unsupportedGrantRevokeStatement}.
	 * @param ctx the parse tree
	 */
	void exitGrantTablePrivilege(DorisParser.GrantTablePrivilegeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code grantResourcePrivilege}
	 * labeled alternative in {@link DorisParser#unsupportedGrantRevokeStatement}.
	 * @param ctx the parse tree
	 */
	void enterGrantResourcePrivilege(DorisParser.GrantResourcePrivilegeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code grantResourcePrivilege}
	 * labeled alternative in {@link DorisParser#unsupportedGrantRevokeStatement}.
	 * @param ctx the parse tree
	 */
	void exitGrantResourcePrivilege(DorisParser.GrantResourcePrivilegeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code grantRole}
	 * labeled alternative in {@link DorisParser#unsupportedGrantRevokeStatement}.
	 * @param ctx the parse tree
	 */
	void enterGrantRole(DorisParser.GrantRoleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code grantRole}
	 * labeled alternative in {@link DorisParser#unsupportedGrantRevokeStatement}.
	 * @param ctx the parse tree
	 */
	void exitGrantRole(DorisParser.GrantRoleContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#privilege}.
	 * @param ctx the parse tree
	 */
	void enterPrivilege(DorisParser.PrivilegeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#privilege}.
	 * @param ctx the parse tree
	 */
	void exitPrivilege(DorisParser.PrivilegeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#privilegeList}.
	 * @param ctx the parse tree
	 */
	void enterPrivilegeList(DorisParser.PrivilegeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#privilegeList}.
	 * @param ctx the parse tree
	 */
	void exitPrivilegeList(DorisParser.PrivilegeListContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterDatabaseProperties}
	 * labeled alternative in {@link DorisParser#unsupportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterDatabaseProperties(DorisParser.AlterDatabasePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterDatabaseProperties}
	 * labeled alternative in {@link DorisParser#unsupportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterDatabaseProperties(DorisParser.AlterDatabasePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterResource}
	 * labeled alternative in {@link DorisParser#unsupportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterResource(DorisParser.AlterResourceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterResource}
	 * labeled alternative in {@link DorisParser#unsupportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterResource(DorisParser.AlterResourceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterColocateGroup}
	 * labeled alternative in {@link DorisParser#unsupportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterColocateGroup(DorisParser.AlterColocateGroupContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterColocateGroup}
	 * labeled alternative in {@link DorisParser#unsupportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterColocateGroup(DorisParser.AlterColocateGroupContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterRoutineLoad(DorisParser.AlterRoutineLoadContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterRoutineLoad}
	 * labeled alternative in {@link DorisParser#unsupportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterRoutineLoad(DorisParser.AlterRoutineLoadContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterStoragePlicy}
	 * labeled alternative in {@link DorisParser#unsupportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterStoragePlicy(DorisParser.AlterStoragePlicyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterStoragePlicy}
	 * labeled alternative in {@link DorisParser#unsupportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterStoragePlicy(DorisParser.AlterStoragePlicyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterUser}
	 * labeled alternative in {@link DorisParser#unsupportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterUser(DorisParser.AlterUserContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterUser}
	 * labeled alternative in {@link DorisParser#unsupportedAlterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterUser(DorisParser.AlterUserContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addBackendClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void enterAddBackendClause(DorisParser.AddBackendClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addBackendClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void exitAddBackendClause(DorisParser.AddBackendClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropBackendClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void enterDropBackendClause(DorisParser.DropBackendClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropBackendClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void exitDropBackendClause(DorisParser.DropBackendClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code decommissionBackendClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void enterDecommissionBackendClause(DorisParser.DecommissionBackendClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code decommissionBackendClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void exitDecommissionBackendClause(DorisParser.DecommissionBackendClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addObserverClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void enterAddObserverClause(DorisParser.AddObserverClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addObserverClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void exitAddObserverClause(DorisParser.AddObserverClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropObserverClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void enterDropObserverClause(DorisParser.DropObserverClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropObserverClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void exitDropObserverClause(DorisParser.DropObserverClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addFollowerClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void enterAddFollowerClause(DorisParser.AddFollowerClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addFollowerClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void exitAddFollowerClause(DorisParser.AddFollowerClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropFollowerClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void enterDropFollowerClause(DorisParser.DropFollowerClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropFollowerClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void exitDropFollowerClause(DorisParser.DropFollowerClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addBrokerClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void enterAddBrokerClause(DorisParser.AddBrokerClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addBrokerClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void exitAddBrokerClause(DorisParser.AddBrokerClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropBrokerClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void enterDropBrokerClause(DorisParser.DropBrokerClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropBrokerClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void exitDropBrokerClause(DorisParser.DropBrokerClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropAllBrokerClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void enterDropAllBrokerClause(DorisParser.DropAllBrokerClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropAllBrokerClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void exitDropAllBrokerClause(DorisParser.DropAllBrokerClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterLoadErrorUrlClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void enterAlterLoadErrorUrlClause(DorisParser.AlterLoadErrorUrlClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterLoadErrorUrlClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void exitAlterLoadErrorUrlClause(DorisParser.AlterLoadErrorUrlClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code modifyBackendClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void enterModifyBackendClause(DorisParser.ModifyBackendClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code modifyBackendClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void exitModifyBackendClause(DorisParser.ModifyBackendClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code modifyFrontendOrBackendHostNameClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void enterModifyFrontendOrBackendHostNameClause(DorisParser.ModifyFrontendOrBackendHostNameClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code modifyFrontendOrBackendHostNameClause}
	 * labeled alternative in {@link DorisParser#alterSystemClause}.
	 * @param ctx the parse tree
	 */
	void exitModifyFrontendOrBackendHostNameClause(DorisParser.ModifyFrontendOrBackendHostNameClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#dropRollupClause}.
	 * @param ctx the parse tree
	 */
	void enterDropRollupClause(DorisParser.DropRollupClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#dropRollupClause}.
	 * @param ctx the parse tree
	 */
	void exitDropRollupClause(DorisParser.DropRollupClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#addRollupClause}.
	 * @param ctx the parse tree
	 */
	void enterAddRollupClause(DorisParser.AddRollupClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#addRollupClause}.
	 * @param ctx the parse tree
	 */
	void exitAddRollupClause(DorisParser.AddRollupClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addColumnClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterAddColumnClause(DorisParser.AddColumnClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addColumnClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitAddColumnClause(DorisParser.AddColumnClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addColumnsClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterAddColumnsClause(DorisParser.AddColumnsClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addColumnsClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitAddColumnsClause(DorisParser.AddColumnsClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropColumnClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterDropColumnClause(DorisParser.DropColumnClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropColumnClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitDropColumnClause(DorisParser.DropColumnClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code modifyColumnClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterModifyColumnClause(DorisParser.ModifyColumnClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code modifyColumnClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitModifyColumnClause(DorisParser.ModifyColumnClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code reorderColumnsClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterReorderColumnsClause(DorisParser.ReorderColumnsClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code reorderColumnsClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitReorderColumnsClause(DorisParser.ReorderColumnsClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addPartitionClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterAddPartitionClause(DorisParser.AddPartitionClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addPartitionClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitAddPartitionClause(DorisParser.AddPartitionClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropPartitionClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterDropPartitionClause(DorisParser.DropPartitionClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropPartitionClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitDropPartitionClause(DorisParser.DropPartitionClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code modifyPartitionClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterModifyPartitionClause(DorisParser.ModifyPartitionClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code modifyPartitionClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitModifyPartitionClause(DorisParser.ModifyPartitionClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code replacePartitionClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterReplacePartitionClause(DorisParser.ReplacePartitionClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code replacePartitionClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitReplacePartitionClause(DorisParser.ReplacePartitionClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code replaceTableClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterReplaceTableClause(DorisParser.ReplaceTableClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code replaceTableClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitReplaceTableClause(DorisParser.ReplaceTableClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code renameClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterRenameClause(DorisParser.RenameClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code renameClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitRenameClause(DorisParser.RenameClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code renameRollupClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterRenameRollupClause(DorisParser.RenameRollupClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code renameRollupClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitRenameRollupClause(DorisParser.RenameRollupClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code renamePartitionClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterRenamePartitionClause(DorisParser.RenamePartitionClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code renamePartitionClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitRenamePartitionClause(DorisParser.RenamePartitionClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code renameColumnClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterRenameColumnClause(DorisParser.RenameColumnClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code renameColumnClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitRenameColumnClause(DorisParser.RenameColumnClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addIndexClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterAddIndexClause(DorisParser.AddIndexClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addIndexClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitAddIndexClause(DorisParser.AddIndexClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropIndexClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterDropIndexClause(DorisParser.DropIndexClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropIndexClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitDropIndexClause(DorisParser.DropIndexClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code enableFeatureClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterEnableFeatureClause(DorisParser.EnableFeatureClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code enableFeatureClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitEnableFeatureClause(DorisParser.EnableFeatureClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code modifyDistributionClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterModifyDistributionClause(DorisParser.ModifyDistributionClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code modifyDistributionClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitModifyDistributionClause(DorisParser.ModifyDistributionClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code modifyTableCommentClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterModifyTableCommentClause(DorisParser.ModifyTableCommentClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code modifyTableCommentClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitModifyTableCommentClause(DorisParser.ModifyTableCommentClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code modifyColumnCommentClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterModifyColumnCommentClause(DorisParser.ModifyColumnCommentClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code modifyColumnCommentClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitModifyColumnCommentClause(DorisParser.ModifyColumnCommentClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code modifyEngineClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterModifyEngineClause(DorisParser.ModifyEngineClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code modifyEngineClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitModifyEngineClause(DorisParser.ModifyEngineClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterMultiPartitionClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void enterAlterMultiPartitionClause(DorisParser.AlterMultiPartitionClauseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterMultiPartitionClause}
	 * labeled alternative in {@link DorisParser#alterTableClause}.
	 * @param ctx the parse tree
	 */
	void exitAlterMultiPartitionClause(DorisParser.AlterMultiPartitionClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#columnPosition}.
	 * @param ctx the parse tree
	 */
	void enterColumnPosition(DorisParser.ColumnPositionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#columnPosition}.
	 * @param ctx the parse tree
	 */
	void exitColumnPosition(DorisParser.ColumnPositionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#toRollup}.
	 * @param ctx the parse tree
	 */
	void enterToRollup(DorisParser.ToRollupContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#toRollup}.
	 * @param ctx the parse tree
	 */
	void exitToRollup(DorisParser.ToRollupContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#fromRollup}.
	 * @param ctx the parse tree
	 */
	void enterFromRollup(DorisParser.FromRollupContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#fromRollup}.
	 * @param ctx the parse tree
	 */
	void exitFromRollup(DorisParser.FromRollupContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropView}
	 * labeled alternative in {@link DorisParser#unsupportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropView(DorisParser.DropViewContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropView}
	 * labeled alternative in {@link DorisParser#unsupportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropView(DorisParser.DropViewContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropResource}
	 * labeled alternative in {@link DorisParser#unsupportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropResource(DorisParser.DropResourceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropResource}
	 * labeled alternative in {@link DorisParser#unsupportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropResource(DorisParser.DropResourceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropRowPolicy}
	 * labeled alternative in {@link DorisParser#unsupportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropRowPolicy(DorisParser.DropRowPolicyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropRowPolicy}
	 * labeled alternative in {@link DorisParser#unsupportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropRowPolicy(DorisParser.DropRowPolicyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropStage}
	 * labeled alternative in {@link DorisParser#unsupportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropStage(DorisParser.DropStageContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropStage}
	 * labeled alternative in {@link DorisParser#unsupportedDropStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropStage(DorisParser.DropStageContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showAnalyze}
	 * labeled alternative in {@link DorisParser#supportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowAnalyze(DorisParser.ShowAnalyzeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showAnalyze}
	 * labeled alternative in {@link DorisParser#supportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowAnalyze(DorisParser.ShowAnalyzeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showQueuedAnalyzeJobs}
	 * labeled alternative in {@link DorisParser#supportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowQueuedAnalyzeJobs(DorisParser.ShowQueuedAnalyzeJobsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showQueuedAnalyzeJobs}
	 * labeled alternative in {@link DorisParser#supportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowQueuedAnalyzeJobs(DorisParser.ShowQueuedAnalyzeJobsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showColumnHistogramStats}
	 * labeled alternative in {@link DorisParser#supportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowColumnHistogramStats(DorisParser.ShowColumnHistogramStatsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showColumnHistogramStats}
	 * labeled alternative in {@link DorisParser#supportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowColumnHistogramStats(DorisParser.ShowColumnHistogramStatsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code analyzeDatabase}
	 * labeled alternative in {@link DorisParser#supportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterAnalyzeDatabase(DorisParser.AnalyzeDatabaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code analyzeDatabase}
	 * labeled alternative in {@link DorisParser#supportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitAnalyzeDatabase(DorisParser.AnalyzeDatabaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code analyzeTable}
	 * labeled alternative in {@link DorisParser#supportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterAnalyzeTable(DorisParser.AnalyzeTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code analyzeTable}
	 * labeled alternative in {@link DorisParser#supportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitAnalyzeTable(DorisParser.AnalyzeTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterTableStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterTableStats(DorisParser.AlterTableStatsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterTableStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterTableStats(DorisParser.AlterTableStatsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterColumnStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterColumnStats(DorisParser.AlterColumnStatsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterColumnStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterColumnStats(DorisParser.AlterColumnStatsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropStats(DorisParser.DropStatsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropStats(DorisParser.DropStatsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropCachedStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropCachedStats(DorisParser.DropCachedStatsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropCachedStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropCachedStats(DorisParser.DropCachedStatsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropExpiredStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropExpiredStats(DorisParser.DropExpiredStatsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropExpiredStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropExpiredStats(DorisParser.DropExpiredStatsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropAanalyzeJob}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropAanalyzeJob(DorisParser.DropAanalyzeJobContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropAanalyzeJob}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropAanalyzeJob(DorisParser.DropAanalyzeJobContext ctx);
	/**
	 * Enter a parse tree produced by the {@code killAnalyzeJob}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterKillAnalyzeJob(DorisParser.KillAnalyzeJobContext ctx);
	/**
	 * Exit a parse tree produced by the {@code killAnalyzeJob}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitKillAnalyzeJob(DorisParser.KillAnalyzeJobContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTableStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTableStats(DorisParser.ShowTableStatsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTableStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTableStats(DorisParser.ShowTableStatsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showIndexStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowIndexStats(DorisParser.ShowIndexStatsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showIndexStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowIndexStats(DorisParser.ShowIndexStatsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showColumnStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowColumnStats(DorisParser.ShowColumnStatsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showColumnStats}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowColumnStats(DorisParser.ShowColumnStatsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showAnalyzeTask}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowAnalyzeTask(DorisParser.ShowAnalyzeTaskContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showAnalyzeTask}
	 * labeled alternative in {@link DorisParser#unsupportedStatsStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowAnalyzeTask(DorisParser.ShowAnalyzeTaskContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#analyzeProperties}.
	 * @param ctx the parse tree
	 */
	void enterAnalyzeProperties(DorisParser.AnalyzePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#analyzeProperties}.
	 * @param ctx the parse tree
	 */
	void exitAnalyzeProperties(DorisParser.AnalyzePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createDatabase}
	 * labeled alternative in {@link DorisParser#unsupportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateDatabase(DorisParser.CreateDatabaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createDatabase}
	 * labeled alternative in {@link DorisParser#unsupportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateDatabase(DorisParser.CreateDatabaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createUser}
	 * labeled alternative in {@link DorisParser#unsupportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateUser(DorisParser.CreateUserContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createUser}
	 * labeled alternative in {@link DorisParser#unsupportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateUser(DorisParser.CreateUserContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createRepository}
	 * labeled alternative in {@link DorisParser#unsupportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateRepository(DorisParser.CreateRepositoryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createRepository}
	 * labeled alternative in {@link DorisParser#unsupportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateRepository(DorisParser.CreateRepositoryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createResource}
	 * labeled alternative in {@link DorisParser#unsupportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateResource(DorisParser.CreateResourceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createResource}
	 * labeled alternative in {@link DorisParser#unsupportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateResource(DorisParser.CreateResourceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createStorageVault}
	 * labeled alternative in {@link DorisParser#unsupportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateStorageVault(DorisParser.CreateStorageVaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createStorageVault}
	 * labeled alternative in {@link DorisParser#unsupportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateStorageVault(DorisParser.CreateStorageVaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createWorkloadPolicy}
	 * labeled alternative in {@link DorisParser#unsupportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateWorkloadPolicy(DorisParser.CreateWorkloadPolicyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createWorkloadPolicy}
	 * labeled alternative in {@link DorisParser#unsupportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateWorkloadPolicy(DorisParser.CreateWorkloadPolicyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createStage}
	 * labeled alternative in {@link DorisParser#unsupportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateStage(DorisParser.CreateStageContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createStage}
	 * labeled alternative in {@link DorisParser#unsupportedCreateStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateStage(DorisParser.CreateStageContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#workloadPolicyActions}.
	 * @param ctx the parse tree
	 */
	void enterWorkloadPolicyActions(DorisParser.WorkloadPolicyActionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#workloadPolicyActions}.
	 * @param ctx the parse tree
	 */
	void exitWorkloadPolicyActions(DorisParser.WorkloadPolicyActionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#workloadPolicyAction}.
	 * @param ctx the parse tree
	 */
	void enterWorkloadPolicyAction(DorisParser.WorkloadPolicyActionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#workloadPolicyAction}.
	 * @param ctx the parse tree
	 */
	void exitWorkloadPolicyAction(DorisParser.WorkloadPolicyActionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#workloadPolicyConditions}.
	 * @param ctx the parse tree
	 */
	void enterWorkloadPolicyConditions(DorisParser.WorkloadPolicyConditionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#workloadPolicyConditions}.
	 * @param ctx the parse tree
	 */
	void exitWorkloadPolicyConditions(DorisParser.WorkloadPolicyConditionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#workloadPolicyCondition}.
	 * @param ctx the parse tree
	 */
	void enterWorkloadPolicyCondition(DorisParser.WorkloadPolicyConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#workloadPolicyCondition}.
	 * @param ctx the parse tree
	 */
	void exitWorkloadPolicyCondition(DorisParser.WorkloadPolicyConditionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#storageBackend}.
	 * @param ctx the parse tree
	 */
	void enterStorageBackend(DorisParser.StorageBackendContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#storageBackend}.
	 * @param ctx the parse tree
	 */
	void exitStorageBackend(DorisParser.StorageBackendContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#passwordOption}.
	 * @param ctx the parse tree
	 */
	void enterPasswordOption(DorisParser.PasswordOptionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#passwordOption}.
	 * @param ctx the parse tree
	 */
	void exitPasswordOption(DorisParser.PasswordOptionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#functionArguments}.
	 * @param ctx the parse tree
	 */
	void enterFunctionArguments(DorisParser.FunctionArgumentsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#functionArguments}.
	 * @param ctx the parse tree
	 */
	void exitFunctionArguments(DorisParser.FunctionArgumentsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#dataTypeList}.
	 * @param ctx the parse tree
	 */
	void enterDataTypeList(DorisParser.DataTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#dataTypeList}.
	 * @param ctx the parse tree
	 */
	void exitDataTypeList(DorisParser.DataTypeListContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setOptions}
	 * labeled alternative in {@link DorisParser#supportedSetStatement}.
	 * @param ctx the parse tree
	 */
	void enterSetOptions(DorisParser.SetOptionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setOptions}
	 * labeled alternative in {@link DorisParser#supportedSetStatement}.
	 * @param ctx the parse tree
	 */
	void exitSetOptions(DorisParser.SetOptionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setDefaultStorageVault}
	 * labeled alternative in {@link DorisParser#supportedSetStatement}.
	 * @param ctx the parse tree
	 */
	void enterSetDefaultStorageVault(DorisParser.SetDefaultStorageVaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setDefaultStorageVault}
	 * labeled alternative in {@link DorisParser#supportedSetStatement}.
	 * @param ctx the parse tree
	 */
	void exitSetDefaultStorageVault(DorisParser.SetDefaultStorageVaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setUserProperties}
	 * labeled alternative in {@link DorisParser#supportedSetStatement}.
	 * @param ctx the parse tree
	 */
	void enterSetUserProperties(DorisParser.SetUserPropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setUserProperties}
	 * labeled alternative in {@link DorisParser#supportedSetStatement}.
	 * @param ctx the parse tree
	 */
	void exitSetUserProperties(DorisParser.SetUserPropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setTransaction}
	 * labeled alternative in {@link DorisParser#supportedSetStatement}.
	 * @param ctx the parse tree
	 */
	void enterSetTransaction(DorisParser.SetTransactionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setTransaction}
	 * labeled alternative in {@link DorisParser#supportedSetStatement}.
	 * @param ctx the parse tree
	 */
	void exitSetTransaction(DorisParser.SetTransactionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setVariableWithType}
	 * labeled alternative in {@link DorisParser#optionWithType}.
	 * @param ctx the parse tree
	 */
	void enterSetVariableWithType(DorisParser.SetVariableWithTypeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setVariableWithType}
	 * labeled alternative in {@link DorisParser#optionWithType}.
	 * @param ctx the parse tree
	 */
	void exitSetVariableWithType(DorisParser.SetVariableWithTypeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setNames}
	 * labeled alternative in {@link DorisParser#optionWithoutType}.
	 * @param ctx the parse tree
	 */
	void enterSetNames(DorisParser.SetNamesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setNames}
	 * labeled alternative in {@link DorisParser#optionWithoutType}.
	 * @param ctx the parse tree
	 */
	void exitSetNames(DorisParser.SetNamesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setCharset}
	 * labeled alternative in {@link DorisParser#optionWithoutType}.
	 * @param ctx the parse tree
	 */
	void enterSetCharset(DorisParser.SetCharsetContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setCharset}
	 * labeled alternative in {@link DorisParser#optionWithoutType}.
	 * @param ctx the parse tree
	 */
	void exitSetCharset(DorisParser.SetCharsetContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setCollate}
	 * labeled alternative in {@link DorisParser#optionWithoutType}.
	 * @param ctx the parse tree
	 */
	void enterSetCollate(DorisParser.SetCollateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setCollate}
	 * labeled alternative in {@link DorisParser#optionWithoutType}.
	 * @param ctx the parse tree
	 */
	void exitSetCollate(DorisParser.SetCollateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setPassword}
	 * labeled alternative in {@link DorisParser#optionWithoutType}.
	 * @param ctx the parse tree
	 */
	void enterSetPassword(DorisParser.SetPasswordContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setPassword}
	 * labeled alternative in {@link DorisParser#optionWithoutType}.
	 * @param ctx the parse tree
	 */
	void exitSetPassword(DorisParser.SetPasswordContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setLdapAdminPassword}
	 * labeled alternative in {@link DorisParser#optionWithoutType}.
	 * @param ctx the parse tree
	 */
	void enterSetLdapAdminPassword(DorisParser.SetLdapAdminPasswordContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setLdapAdminPassword}
	 * labeled alternative in {@link DorisParser#optionWithoutType}.
	 * @param ctx the parse tree
	 */
	void exitSetLdapAdminPassword(DorisParser.SetLdapAdminPasswordContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setVariableWithoutType}
	 * labeled alternative in {@link DorisParser#optionWithoutType}.
	 * @param ctx the parse tree
	 */
	void enterSetVariableWithoutType(DorisParser.SetVariableWithoutTypeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setVariableWithoutType}
	 * labeled alternative in {@link DorisParser#optionWithoutType}.
	 * @param ctx the parse tree
	 */
	void exitSetVariableWithoutType(DorisParser.SetVariableWithoutTypeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setSystemVariable}
	 * labeled alternative in {@link DorisParser#variable}.
	 * @param ctx the parse tree
	 */
	void enterSetSystemVariable(DorisParser.SetSystemVariableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setSystemVariable}
	 * labeled alternative in {@link DorisParser#variable}.
	 * @param ctx the parse tree
	 */
	void exitSetSystemVariable(DorisParser.SetSystemVariableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setUserVariable}
	 * labeled alternative in {@link DorisParser#variable}.
	 * @param ctx the parse tree
	 */
	void enterSetUserVariable(DorisParser.SetUserVariableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setUserVariable}
	 * labeled alternative in {@link DorisParser#variable}.
	 * @param ctx the parse tree
	 */
	void exitSetUserVariable(DorisParser.SetUserVariableContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#transactionAccessMode}.
	 * @param ctx the parse tree
	 */
	void enterTransactionAccessMode(DorisParser.TransactionAccessModeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#transactionAccessMode}.
	 * @param ctx the parse tree
	 */
	void exitTransactionAccessMode(DorisParser.TransactionAccessModeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#isolationLevel}.
	 * @param ctx the parse tree
	 */
	void enterIsolationLevel(DorisParser.IsolationLevelContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#isolationLevel}.
	 * @param ctx the parse tree
	 */
	void exitIsolationLevel(DorisParser.IsolationLevelContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#supportedUnsetStatement}.
	 * @param ctx the parse tree
	 */
	void enterSupportedUnsetStatement(DorisParser.SupportedUnsetStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#supportedUnsetStatement}.
	 * @param ctx the parse tree
	 */
	void exitSupportedUnsetStatement(DorisParser.SupportedUnsetStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code switchCatalog}
	 * labeled alternative in {@link DorisParser#supportedUseStatement}.
	 * @param ctx the parse tree
	 */
	void enterSwitchCatalog(DorisParser.SwitchCatalogContext ctx);
	/**
	 * Exit a parse tree produced by the {@code switchCatalog}
	 * labeled alternative in {@link DorisParser#supportedUseStatement}.
	 * @param ctx the parse tree
	 */
	void exitSwitchCatalog(DorisParser.SwitchCatalogContext ctx);
	/**
	 * Enter a parse tree produced by the {@code useDatabase}
	 * labeled alternative in {@link DorisParser#supportedUseStatement}.
	 * @param ctx the parse tree
	 */
	void enterUseDatabase(DorisParser.UseDatabaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code useDatabase}
	 * labeled alternative in {@link DorisParser#supportedUseStatement}.
	 * @param ctx the parse tree
	 */
	void exitUseDatabase(DorisParser.UseDatabaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code useCloudCluster}
	 * labeled alternative in {@link DorisParser#unsupportedUseStatement}.
	 * @param ctx the parse tree
	 */
	void enterUseCloudCluster(DorisParser.UseCloudClusterContext ctx);
	/**
	 * Exit a parse tree produced by the {@code useCloudCluster}
	 * labeled alternative in {@link DorisParser#unsupportedUseStatement}.
	 * @param ctx the parse tree
	 */
	void exitUseCloudCluster(DorisParser.UseCloudClusterContext ctx);
	/**
	 * Enter a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link DorisParser#unsupportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void enterTruncateTable(DorisParser.TruncateTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link DorisParser#unsupportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void exitTruncateTable(DorisParser.TruncateTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code copyInto}
	 * labeled alternative in {@link DorisParser#unsupportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void enterCopyInto(DorisParser.CopyIntoContext ctx);
	/**
	 * Exit a parse tree produced by the {@code copyInto}
	 * labeled alternative in {@link DorisParser#unsupportedDmlStatement}.
	 * @param ctx the parse tree
	 */
	void exitCopyInto(DorisParser.CopyIntoContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#stageAndPattern}.
	 * @param ctx the parse tree
	 */
	void enterStageAndPattern(DorisParser.StageAndPatternContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#stageAndPattern}.
	 * @param ctx the parse tree
	 */
	void exitStageAndPattern(DorisParser.StageAndPatternContext ctx);
	/**
	 * Enter a parse tree produced by the {@code killConnection}
	 * labeled alternative in {@link DorisParser#unsupportedKillStatement}.
	 * @param ctx the parse tree
	 */
	void enterKillConnection(DorisParser.KillConnectionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code killConnection}
	 * labeled alternative in {@link DorisParser#unsupportedKillStatement}.
	 * @param ctx the parse tree
	 */
	void exitKillConnection(DorisParser.KillConnectionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code killQuery}
	 * labeled alternative in {@link DorisParser#unsupportedKillStatement}.
	 * @param ctx the parse tree
	 */
	void enterKillQuery(DorisParser.KillQueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code killQuery}
	 * labeled alternative in {@link DorisParser#unsupportedKillStatement}.
	 * @param ctx the parse tree
	 */
	void exitKillQuery(DorisParser.KillQueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code describeTableValuedFunction}
	 * labeled alternative in {@link DorisParser#supportedDescribeStatement}.
	 * @param ctx the parse tree
	 */
	void enterDescribeTableValuedFunction(DorisParser.DescribeTableValuedFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code describeTableValuedFunction}
	 * labeled alternative in {@link DorisParser#supportedDescribeStatement}.
	 * @param ctx the parse tree
	 */
	void exitDescribeTableValuedFunction(DorisParser.DescribeTableValuedFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code describeTableAll}
	 * labeled alternative in {@link DorisParser#supportedDescribeStatement}.
	 * @param ctx the parse tree
	 */
	void enterDescribeTableAll(DorisParser.DescribeTableAllContext ctx);
	/**
	 * Exit a parse tree produced by the {@code describeTableAll}
	 * labeled alternative in {@link DorisParser#supportedDescribeStatement}.
	 * @param ctx the parse tree
	 */
	void exitDescribeTableAll(DorisParser.DescribeTableAllContext ctx);
	/**
	 * Enter a parse tree produced by the {@code describeTable}
	 * labeled alternative in {@link DorisParser#supportedDescribeStatement}.
	 * @param ctx the parse tree
	 */
	void enterDescribeTable(DorisParser.DescribeTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code describeTable}
	 * labeled alternative in {@link DorisParser#supportedDescribeStatement}.
	 * @param ctx the parse tree
	 */
	void exitDescribeTable(DorisParser.DescribeTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#constraint}.
	 * @param ctx the parse tree
	 */
	void enterConstraint(DorisParser.ConstraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#constraint}.
	 * @param ctx the parse tree
	 */
	void exitConstraint(DorisParser.ConstraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#partitionSpec}.
	 * @param ctx the parse tree
	 */
	void enterPartitionSpec(DorisParser.PartitionSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#partitionSpec}.
	 * @param ctx the parse tree
	 */
	void exitPartitionSpec(DorisParser.PartitionSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#partitionTable}.
	 * @param ctx the parse tree
	 */
	void enterPartitionTable(DorisParser.PartitionTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#partitionTable}.
	 * @param ctx the parse tree
	 */
	void exitPartitionTable(DorisParser.PartitionTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#identityOrFunctionList}.
	 * @param ctx the parse tree
	 */
	void enterIdentityOrFunctionList(DorisParser.IdentityOrFunctionListContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#identityOrFunctionList}.
	 * @param ctx the parse tree
	 */
	void exitIdentityOrFunctionList(DorisParser.IdentityOrFunctionListContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#identityOrFunction}.
	 * @param ctx the parse tree
	 */
	void enterIdentityOrFunction(DorisParser.IdentityOrFunctionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#identityOrFunction}.
	 * @param ctx the parse tree
	 */
	void exitIdentityOrFunction(DorisParser.IdentityOrFunctionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#dataDesc}.
	 * @param ctx the parse tree
	 */
	void enterDataDesc(DorisParser.DataDescContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#dataDesc}.
	 * @param ctx the parse tree
	 */
	void exitDataDesc(DorisParser.DataDescContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#statementScope}.
	 * @param ctx the parse tree
	 */
	void enterStatementScope(DorisParser.StatementScopeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#statementScope}.
	 * @param ctx the parse tree
	 */
	void exitStatementScope(DorisParser.StatementScopeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#buildMode}.
	 * @param ctx the parse tree
	 */
	void enterBuildMode(DorisParser.BuildModeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#buildMode}.
	 * @param ctx the parse tree
	 */
	void exitBuildMode(DorisParser.BuildModeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#refreshTrigger}.
	 * @param ctx the parse tree
	 */
	void enterRefreshTrigger(DorisParser.RefreshTriggerContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#refreshTrigger}.
	 * @param ctx the parse tree
	 */
	void exitRefreshTrigger(DorisParser.RefreshTriggerContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#refreshSchedule}.
	 * @param ctx the parse tree
	 */
	void enterRefreshSchedule(DorisParser.RefreshScheduleContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#refreshSchedule}.
	 * @param ctx the parse tree
	 */
	void exitRefreshSchedule(DorisParser.RefreshScheduleContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#refreshMethod}.
	 * @param ctx the parse tree
	 */
	void enterRefreshMethod(DorisParser.RefreshMethodContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#refreshMethod}.
	 * @param ctx the parse tree
	 */
	void exitRefreshMethod(DorisParser.RefreshMethodContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#mvPartition}.
	 * @param ctx the parse tree
	 */
	void enterMvPartition(DorisParser.MvPartitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#mvPartition}.
	 * @param ctx the parse tree
	 */
	void exitMvPartition(DorisParser.MvPartitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#identifierOrText}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierOrText(DorisParser.IdentifierOrTextContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#identifierOrText}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierOrText(DorisParser.IdentifierOrTextContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#identifierOrTextOrAsterisk}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierOrTextOrAsterisk(DorisParser.IdentifierOrTextOrAsteriskContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#identifierOrTextOrAsterisk}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierOrTextOrAsterisk(DorisParser.IdentifierOrTextOrAsteriskContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#multipartIdentifierOrAsterisk}.
	 * @param ctx the parse tree
	 */
	void enterMultipartIdentifierOrAsterisk(DorisParser.MultipartIdentifierOrAsteriskContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#multipartIdentifierOrAsterisk}.
	 * @param ctx the parse tree
	 */
	void exitMultipartIdentifierOrAsterisk(DorisParser.MultipartIdentifierOrAsteriskContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#identifierOrAsterisk}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierOrAsterisk(DorisParser.IdentifierOrAsteriskContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#identifierOrAsterisk}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierOrAsterisk(DorisParser.IdentifierOrAsteriskContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#userIdentify}.
	 * @param ctx the parse tree
	 */
	void enterUserIdentify(DorisParser.UserIdentifyContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#userIdentify}.
	 * @param ctx the parse tree
	 */
	void exitUserIdentify(DorisParser.UserIdentifyContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#grantUserIdentify}.
	 * @param ctx the parse tree
	 */
	void enterGrantUserIdentify(DorisParser.GrantUserIdentifyContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#grantUserIdentify}.
	 * @param ctx the parse tree
	 */
	void exitGrantUserIdentify(DorisParser.GrantUserIdentifyContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#explain}.
	 * @param ctx the parse tree
	 */
	void enterExplain(DorisParser.ExplainContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#explain}.
	 * @param ctx the parse tree
	 */
	void exitExplain(DorisParser.ExplainContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#explainCommand}.
	 * @param ctx the parse tree
	 */
	void enterExplainCommand(DorisParser.ExplainCommandContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#explainCommand}.
	 * @param ctx the parse tree
	 */
	void exitExplainCommand(DorisParser.ExplainCommandContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#planType}.
	 * @param ctx the parse tree
	 */
	void enterPlanType(DorisParser.PlanTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#planType}.
	 * @param ctx the parse tree
	 */
	void exitPlanType(DorisParser.PlanTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#replayCommand}.
	 * @param ctx the parse tree
	 */
	void enterReplayCommand(DorisParser.ReplayCommandContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#replayCommand}.
	 * @param ctx the parse tree
	 */
	void exitReplayCommand(DorisParser.ReplayCommandContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#replayType}.
	 * @param ctx the parse tree
	 */
	void enterReplayType(DorisParser.ReplayTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#replayType}.
	 * @param ctx the parse tree
	 */
	void exitReplayType(DorisParser.ReplayTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#mergeType}.
	 * @param ctx the parse tree
	 */
	void enterMergeType(DorisParser.MergeTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#mergeType}.
	 * @param ctx the parse tree
	 */
	void exitMergeType(DorisParser.MergeTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#preFilterClause}.
	 * @param ctx the parse tree
	 */
	void enterPreFilterClause(DorisParser.PreFilterClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#preFilterClause}.
	 * @param ctx the parse tree
	 */
	void exitPreFilterClause(DorisParser.PreFilterClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#deleteOnClause}.
	 * @param ctx the parse tree
	 */
	void enterDeleteOnClause(DorisParser.DeleteOnClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#deleteOnClause}.
	 * @param ctx the parse tree
	 */
	void exitDeleteOnClause(DorisParser.DeleteOnClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#sequenceColClause}.
	 * @param ctx the parse tree
	 */
	void enterSequenceColClause(DorisParser.SequenceColClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#sequenceColClause}.
	 * @param ctx the parse tree
	 */
	void exitSequenceColClause(DorisParser.SequenceColClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#colFromPath}.
	 * @param ctx the parse tree
	 */
	void enterColFromPath(DorisParser.ColFromPathContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#colFromPath}.
	 * @param ctx the parse tree
	 */
	void exitColFromPath(DorisParser.ColFromPathContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#colMappingList}.
	 * @param ctx the parse tree
	 */
	void enterColMappingList(DorisParser.ColMappingListContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#colMappingList}.
	 * @param ctx the parse tree
	 */
	void exitColMappingList(DorisParser.ColMappingListContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#mappingExpr}.
	 * @param ctx the parse tree
	 */
	void enterMappingExpr(DorisParser.MappingExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#mappingExpr}.
	 * @param ctx the parse tree
	 */
	void exitMappingExpr(DorisParser.MappingExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#withRemoteStorageSystem}.
	 * @param ctx the parse tree
	 */
	void enterWithRemoteStorageSystem(DorisParser.WithRemoteStorageSystemContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#withRemoteStorageSystem}.
	 * @param ctx the parse tree
	 */
	void exitWithRemoteStorageSystem(DorisParser.WithRemoteStorageSystemContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#resourceDesc}.
	 * @param ctx the parse tree
	 */
	void enterResourceDesc(DorisParser.ResourceDescContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#resourceDesc}.
	 * @param ctx the parse tree
	 */
	void exitResourceDesc(DorisParser.ResourceDescContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#mysqlDataDesc}.
	 * @param ctx the parse tree
	 */
	void enterMysqlDataDesc(DorisParser.MysqlDataDescContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#mysqlDataDesc}.
	 * @param ctx the parse tree
	 */
	void exitMysqlDataDesc(DorisParser.MysqlDataDescContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#skipLines}.
	 * @param ctx the parse tree
	 */
	void enterSkipLines(DorisParser.SkipLinesContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#skipLines}.
	 * @param ctx the parse tree
	 */
	void exitSkipLines(DorisParser.SkipLinesContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#outFileClause}.
	 * @param ctx the parse tree
	 */
	void enterOutFileClause(DorisParser.OutFileClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#outFileClause}.
	 * @param ctx the parse tree
	 */
	void exitOutFileClause(DorisParser.OutFileClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#query}.
	 * @param ctx the parse tree
	 */
	void enterQuery(DorisParser.QueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#query}.
	 * @param ctx the parse tree
	 */
	void exitQuery(DorisParser.QueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code queryTermDefault}
	 * labeled alternative in {@link DorisParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void enterQueryTermDefault(DorisParser.QueryTermDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code queryTermDefault}
	 * labeled alternative in {@link DorisParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void exitQueryTermDefault(DorisParser.QueryTermDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link DorisParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void enterSetOperation(DorisParser.SetOperationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link DorisParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void exitSetOperation(DorisParser.SetOperationContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
	void enterSetQuantifier(DorisParser.SetQuantifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
	void exitSetQuantifier(DorisParser.SetQuantifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link DorisParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterQueryPrimaryDefault(DorisParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link DorisParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitQueryPrimaryDefault(DorisParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link DorisParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterSubquery(DorisParser.SubqueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link DorisParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitSubquery(DorisParser.SubqueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code valuesTable}
	 * labeled alternative in {@link DorisParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterValuesTable(DorisParser.ValuesTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code valuesTable}
	 * labeled alternative in {@link DorisParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitValuesTable(DorisParser.ValuesTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code regularQuerySpecification}
	 * labeled alternative in {@link DorisParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	void enterRegularQuerySpecification(DorisParser.RegularQuerySpecificationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code regularQuerySpecification}
	 * labeled alternative in {@link DorisParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	void exitRegularQuerySpecification(DorisParser.RegularQuerySpecificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#cte}.
	 * @param ctx the parse tree
	 */
	void enterCte(DorisParser.CteContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#cte}.
	 * @param ctx the parse tree
	 */
	void exitCte(DorisParser.CteContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#aliasQuery}.
	 * @param ctx the parse tree
	 */
	void enterAliasQuery(DorisParser.AliasQueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#aliasQuery}.
	 * @param ctx the parse tree
	 */
	void exitAliasQuery(DorisParser.AliasQueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#columnAliases}.
	 * @param ctx the parse tree
	 */
	void enterColumnAliases(DorisParser.ColumnAliasesContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#columnAliases}.
	 * @param ctx the parse tree
	 */
	void exitColumnAliases(DorisParser.ColumnAliasesContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void enterSelectClause(DorisParser.SelectClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void exitSelectClause(DorisParser.SelectClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#selectColumnClause}.
	 * @param ctx the parse tree
	 */
	void enterSelectColumnClause(DorisParser.SelectColumnClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#selectColumnClause}.
	 * @param ctx the parse tree
	 */
	void exitSelectColumnClause(DorisParser.SelectColumnClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void enterWhereClause(DorisParser.WhereClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void exitWhereClause(DorisParser.WhereClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void enterFromClause(DorisParser.FromClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void exitFromClause(DorisParser.FromClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#intoClause}.
	 * @param ctx the parse tree
	 */
	void enterIntoClause(DorisParser.IntoClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#intoClause}.
	 * @param ctx the parse tree
	 */
	void exitIntoClause(DorisParser.IntoClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#bulkCollectClause}.
	 * @param ctx the parse tree
	 */
	void enterBulkCollectClause(DorisParser.BulkCollectClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#bulkCollectClause}.
	 * @param ctx the parse tree
	 */
	void exitBulkCollectClause(DorisParser.BulkCollectClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#tableRow}.
	 * @param ctx the parse tree
	 */
	void enterTableRow(DorisParser.TableRowContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#tableRow}.
	 * @param ctx the parse tree
	 */
	void exitTableRow(DorisParser.TableRowContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#relations}.
	 * @param ctx the parse tree
	 */
	void enterRelations(DorisParser.RelationsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#relations}.
	 * @param ctx the parse tree
	 */
	void exitRelations(DorisParser.RelationsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#relation}.
	 * @param ctx the parse tree
	 */
	void enterRelation(DorisParser.RelationContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#relation}.
	 * @param ctx the parse tree
	 */
	void exitRelation(DorisParser.RelationContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#joinRelation}.
	 * @param ctx the parse tree
	 */
	void enterJoinRelation(DorisParser.JoinRelationContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#joinRelation}.
	 * @param ctx the parse tree
	 */
	void exitJoinRelation(DorisParser.JoinRelationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bracketDistributeType}
	 * labeled alternative in {@link DorisParser#distributeType}.
	 * @param ctx the parse tree
	 */
	void enterBracketDistributeType(DorisParser.BracketDistributeTypeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bracketDistributeType}
	 * labeled alternative in {@link DorisParser#distributeType}.
	 * @param ctx the parse tree
	 */
	void exitBracketDistributeType(DorisParser.BracketDistributeTypeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code commentDistributeType}
	 * labeled alternative in {@link DorisParser#distributeType}.
	 * @param ctx the parse tree
	 */
	void enterCommentDistributeType(DorisParser.CommentDistributeTypeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code commentDistributeType}
	 * labeled alternative in {@link DorisParser#distributeType}.
	 * @param ctx the parse tree
	 */
	void exitCommentDistributeType(DorisParser.CommentDistributeTypeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bracketRelationHint}
	 * labeled alternative in {@link DorisParser#relationHint}.
	 * @param ctx the parse tree
	 */
	void enterBracketRelationHint(DorisParser.BracketRelationHintContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bracketRelationHint}
	 * labeled alternative in {@link DorisParser#relationHint}.
	 * @param ctx the parse tree
	 */
	void exitBracketRelationHint(DorisParser.BracketRelationHintContext ctx);
	/**
	 * Enter a parse tree produced by the {@code commentRelationHint}
	 * labeled alternative in {@link DorisParser#relationHint}.
	 * @param ctx the parse tree
	 */
	void enterCommentRelationHint(DorisParser.CommentRelationHintContext ctx);
	/**
	 * Exit a parse tree produced by the {@code commentRelationHint}
	 * labeled alternative in {@link DorisParser#relationHint}.
	 * @param ctx the parse tree
	 */
	void exitCommentRelationHint(DorisParser.CommentRelationHintContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#aggClause}.
	 * @param ctx the parse tree
	 */
	void enterAggClause(DorisParser.AggClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#aggClause}.
	 * @param ctx the parse tree
	 */
	void exitAggClause(DorisParser.AggClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void enterGroupingElement(DorisParser.GroupingElementContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void exitGroupingElement(DorisParser.GroupingElementContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	void enterGroupingSet(DorisParser.GroupingSetContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	void exitGroupingSet(DorisParser.GroupingSetContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void enterHavingClause(DorisParser.HavingClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void exitHavingClause(DorisParser.HavingClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#qualifyClause}.
	 * @param ctx the parse tree
	 */
	void enterQualifyClause(DorisParser.QualifyClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#qualifyClause}.
	 * @param ctx the parse tree
	 */
	void exitQualifyClause(DorisParser.QualifyClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#selectHint}.
	 * @param ctx the parse tree
	 */
	void enterSelectHint(DorisParser.SelectHintContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#selectHint}.
	 * @param ctx the parse tree
	 */
	void exitSelectHint(DorisParser.SelectHintContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#hintStatement}.
	 * @param ctx the parse tree
	 */
	void enterHintStatement(DorisParser.HintStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#hintStatement}.
	 * @param ctx the parse tree
	 */
	void exitHintStatement(DorisParser.HintStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#hintAssignment}.
	 * @param ctx the parse tree
	 */
	void enterHintAssignment(DorisParser.HintAssignmentContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#hintAssignment}.
	 * @param ctx the parse tree
	 */
	void exitHintAssignment(DorisParser.HintAssignmentContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#updateAssignment}.
	 * @param ctx the parse tree
	 */
	void enterUpdateAssignment(DorisParser.UpdateAssignmentContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#updateAssignment}.
	 * @param ctx the parse tree
	 */
	void exitUpdateAssignment(DorisParser.UpdateAssignmentContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#updateAssignmentSeq}.
	 * @param ctx the parse tree
	 */
	void enterUpdateAssignmentSeq(DorisParser.UpdateAssignmentSeqContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#updateAssignmentSeq}.
	 * @param ctx the parse tree
	 */
	void exitUpdateAssignmentSeq(DorisParser.UpdateAssignmentSeqContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#lateralView}.
	 * @param ctx the parse tree
	 */
	void enterLateralView(DorisParser.LateralViewContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#lateralView}.
	 * @param ctx the parse tree
	 */
	void exitLateralView(DorisParser.LateralViewContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#queryOrganization}.
	 * @param ctx the parse tree
	 */
	void enterQueryOrganization(DorisParser.QueryOrganizationContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#queryOrganization}.
	 * @param ctx the parse tree
	 */
	void exitQueryOrganization(DorisParser.QueryOrganizationContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#sortClause}.
	 * @param ctx the parse tree
	 */
	void enterSortClause(DorisParser.SortClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#sortClause}.
	 * @param ctx the parse tree
	 */
	void exitSortClause(DorisParser.SortClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#sortItem}.
	 * @param ctx the parse tree
	 */
	void enterSortItem(DorisParser.SortItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#sortItem}.
	 * @param ctx the parse tree
	 */
	void exitSortItem(DorisParser.SortItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#limitClause}.
	 * @param ctx the parse tree
	 */
	void enterLimitClause(DorisParser.LimitClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#limitClause}.
	 * @param ctx the parse tree
	 */
	void exitLimitClause(DorisParser.LimitClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#partitionClause}.
	 * @param ctx the parse tree
	 */
	void enterPartitionClause(DorisParser.PartitionClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#partitionClause}.
	 * @param ctx the parse tree
	 */
	void exitPartitionClause(DorisParser.PartitionClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#joinType}.
	 * @param ctx the parse tree
	 */
	void enterJoinType(DorisParser.JoinTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#joinType}.
	 * @param ctx the parse tree
	 */
	void exitJoinType(DorisParser.JoinTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
	void enterJoinCriteria(DorisParser.JoinCriteriaContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
	void exitJoinCriteria(DorisParser.JoinCriteriaContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#identifierList}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierList(DorisParser.IdentifierListContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#identifierList}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierList(DorisParser.IdentifierListContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierSeq(DorisParser.IdentifierSeqContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierSeq(DorisParser.IdentifierSeqContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#optScanParams}.
	 * @param ctx the parse tree
	 */
	void enterOptScanParams(DorisParser.OptScanParamsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#optScanParams}.
	 * @param ctx the parse tree
	 */
	void exitOptScanParams(DorisParser.OptScanParamsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link DorisParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterTableName(DorisParser.TableNameContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link DorisParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitTableName(DorisParser.TableNameContext ctx);
	/**
	 * Enter a parse tree produced by the {@code aliasedQuery}
	 * labeled alternative in {@link DorisParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterAliasedQuery(DorisParser.AliasedQueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code aliasedQuery}
	 * labeled alternative in {@link DorisParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitAliasedQuery(DorisParser.AliasedQueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tableValuedFunction}
	 * labeled alternative in {@link DorisParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterTableValuedFunction(DorisParser.TableValuedFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tableValuedFunction}
	 * labeled alternative in {@link DorisParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitTableValuedFunction(DorisParser.TableValuedFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code relationList}
	 * labeled alternative in {@link DorisParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterRelationList(DorisParser.RelationListContext ctx);
	/**
	 * Exit a parse tree produced by the {@code relationList}
	 * labeled alternative in {@link DorisParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitRelationList(DorisParser.RelationListContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#materializedViewName}.
	 * @param ctx the parse tree
	 */
	void enterMaterializedViewName(DorisParser.MaterializedViewNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#materializedViewName}.
	 * @param ctx the parse tree
	 */
	void exitMaterializedViewName(DorisParser.MaterializedViewNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#propertyClause}.
	 * @param ctx the parse tree
	 */
	void enterPropertyClause(DorisParser.PropertyClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#propertyClause}.
	 * @param ctx the parse tree
	 */
	void exitPropertyClause(DorisParser.PropertyClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#propertyItemList}.
	 * @param ctx the parse tree
	 */
	void enterPropertyItemList(DorisParser.PropertyItemListContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#propertyItemList}.
	 * @param ctx the parse tree
	 */
	void exitPropertyItemList(DorisParser.PropertyItemListContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#propertyItem}.
	 * @param ctx the parse tree
	 */
	void enterPropertyItem(DorisParser.PropertyItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#propertyItem}.
	 * @param ctx the parse tree
	 */
	void exitPropertyItem(DorisParser.PropertyItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#propertyKey}.
	 * @param ctx the parse tree
	 */
	void enterPropertyKey(DorisParser.PropertyKeyContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#propertyKey}.
	 * @param ctx the parse tree
	 */
	void exitPropertyKey(DorisParser.PropertyKeyContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	void enterPropertyValue(DorisParser.PropertyValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	void exitPropertyValue(DorisParser.PropertyValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#tableAlias}.
	 * @param ctx the parse tree
	 */
	void enterTableAlias(DorisParser.TableAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#tableAlias}.
	 * @param ctx the parse tree
	 */
	void exitTableAlias(DorisParser.TableAliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#multipartIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterMultipartIdentifier(DorisParser.MultipartIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#multipartIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitMultipartIdentifier(DorisParser.MultipartIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#simpleColumnDefs}.
	 * @param ctx the parse tree
	 */
	void enterSimpleColumnDefs(DorisParser.SimpleColumnDefsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#simpleColumnDefs}.
	 * @param ctx the parse tree
	 */
	void exitSimpleColumnDefs(DorisParser.SimpleColumnDefsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#simpleColumnDef}.
	 * @param ctx the parse tree
	 */
	void enterSimpleColumnDef(DorisParser.SimpleColumnDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#simpleColumnDef}.
	 * @param ctx the parse tree
	 */
	void exitSimpleColumnDef(DorisParser.SimpleColumnDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#columnDefs}.
	 * @param ctx the parse tree
	 */
	void enterColumnDefs(DorisParser.ColumnDefsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#columnDefs}.
	 * @param ctx the parse tree
	 */
	void exitColumnDefs(DorisParser.ColumnDefsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#columnDef}.
	 * @param ctx the parse tree
	 */
	void enterColumnDef(DorisParser.ColumnDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#columnDef}.
	 * @param ctx the parse tree
	 */
	void exitColumnDef(DorisParser.ColumnDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#indexDefs}.
	 * @param ctx the parse tree
	 */
	void enterIndexDefs(DorisParser.IndexDefsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#indexDefs}.
	 * @param ctx the parse tree
	 */
	void exitIndexDefs(DorisParser.IndexDefsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#indexDef}.
	 * @param ctx the parse tree
	 */
	void enterIndexDef(DorisParser.IndexDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#indexDef}.
	 * @param ctx the parse tree
	 */
	void exitIndexDef(DorisParser.IndexDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#partitionsDef}.
	 * @param ctx the parse tree
	 */
	void enterPartitionsDef(DorisParser.PartitionsDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#partitionsDef}.
	 * @param ctx the parse tree
	 */
	void exitPartitionsDef(DorisParser.PartitionsDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#partitionDef}.
	 * @param ctx the parse tree
	 */
	void enterPartitionDef(DorisParser.PartitionDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#partitionDef}.
	 * @param ctx the parse tree
	 */
	void exitPartitionDef(DorisParser.PartitionDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#lessThanPartitionDef}.
	 * @param ctx the parse tree
	 */
	void enterLessThanPartitionDef(DorisParser.LessThanPartitionDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#lessThanPartitionDef}.
	 * @param ctx the parse tree
	 */
	void exitLessThanPartitionDef(DorisParser.LessThanPartitionDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#fixedPartitionDef}.
	 * @param ctx the parse tree
	 */
	void enterFixedPartitionDef(DorisParser.FixedPartitionDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#fixedPartitionDef}.
	 * @param ctx the parse tree
	 */
	void exitFixedPartitionDef(DorisParser.FixedPartitionDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#stepPartitionDef}.
	 * @param ctx the parse tree
	 */
	void enterStepPartitionDef(DorisParser.StepPartitionDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#stepPartitionDef}.
	 * @param ctx the parse tree
	 */
	void exitStepPartitionDef(DorisParser.StepPartitionDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#inPartitionDef}.
	 * @param ctx the parse tree
	 */
	void enterInPartitionDef(DorisParser.InPartitionDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#inPartitionDef}.
	 * @param ctx the parse tree
	 */
	void exitInPartitionDef(DorisParser.InPartitionDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#partitionValueList}.
	 * @param ctx the parse tree
	 */
	void enterPartitionValueList(DorisParser.PartitionValueListContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#partitionValueList}.
	 * @param ctx the parse tree
	 */
	void exitPartitionValueList(DorisParser.PartitionValueListContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#partitionValueDef}.
	 * @param ctx the parse tree
	 */
	void enterPartitionValueDef(DorisParser.PartitionValueDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#partitionValueDef}.
	 * @param ctx the parse tree
	 */
	void exitPartitionValueDef(DorisParser.PartitionValueDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#rollupDefs}.
	 * @param ctx the parse tree
	 */
	void enterRollupDefs(DorisParser.RollupDefsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#rollupDefs}.
	 * @param ctx the parse tree
	 */
	void exitRollupDefs(DorisParser.RollupDefsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#rollupDef}.
	 * @param ctx the parse tree
	 */
	void enterRollupDef(DorisParser.RollupDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#rollupDef}.
	 * @param ctx the parse tree
	 */
	void exitRollupDef(DorisParser.RollupDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#aggTypeDef}.
	 * @param ctx the parse tree
	 */
	void enterAggTypeDef(DorisParser.AggTypeDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#aggTypeDef}.
	 * @param ctx the parse tree
	 */
	void exitAggTypeDef(DorisParser.AggTypeDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#tabletList}.
	 * @param ctx the parse tree
	 */
	void enterTabletList(DorisParser.TabletListContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#tabletList}.
	 * @param ctx the parse tree
	 */
	void exitTabletList(DorisParser.TabletListContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#inlineTable}.
	 * @param ctx the parse tree
	 */
	void enterInlineTable(DorisParser.InlineTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#inlineTable}.
	 * @param ctx the parse tree
	 */
	void exitInlineTable(DorisParser.InlineTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#namedExpression}.
	 * @param ctx the parse tree
	 */
	void enterNamedExpression(DorisParser.NamedExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#namedExpression}.
	 * @param ctx the parse tree
	 */
	void exitNamedExpression(DorisParser.NamedExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#namedExpressionSeq}.
	 * @param ctx the parse tree
	 */
	void enterNamedExpressionSeq(DorisParser.NamedExpressionSeqContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#namedExpressionSeq}.
	 * @param ctx the parse tree
	 */
	void exitNamedExpressionSeq(DorisParser.NamedExpressionSeqContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(DorisParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(DorisParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#lambdaExpression}.
	 * @param ctx the parse tree
	 */
	void enterLambdaExpression(DorisParser.LambdaExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#lambdaExpression}.
	 * @param ctx the parse tree
	 */
	void exitLambdaExpression(DorisParser.LambdaExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code exist}
	 * labeled alternative in {@link DorisParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterExist(DorisParser.ExistContext ctx);
	/**
	 * Exit a parse tree produced by the {@code exist}
	 * labeled alternative in {@link DorisParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitExist(DorisParser.ExistContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link DorisParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogicalNot(DorisParser.LogicalNotContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link DorisParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogicalNot(DorisParser.LogicalNotContext ctx);
	/**
	 * Enter a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link DorisParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterPredicated(DorisParser.PredicatedContext ctx);
	/**
	 * Exit a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link DorisParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitPredicated(DorisParser.PredicatedContext ctx);
	/**
	 * Enter a parse tree produced by the {@code isnull}
	 * labeled alternative in {@link DorisParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterIsnull(DorisParser.IsnullContext ctx);
	/**
	 * Exit a parse tree produced by the {@code isnull}
	 * labeled alternative in {@link DorisParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitIsnull(DorisParser.IsnullContext ctx);
	/**
	 * Enter a parse tree produced by the {@code is_not_null_pred}
	 * labeled alternative in {@link DorisParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterIs_not_null_pred(DorisParser.Is_not_null_predContext ctx);
	/**
	 * Exit a parse tree produced by the {@code is_not_null_pred}
	 * labeled alternative in {@link DorisParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitIs_not_null_pred(DorisParser.Is_not_null_predContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link DorisParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogicalBinary(DorisParser.LogicalBinaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link DorisParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogicalBinary(DorisParser.LogicalBinaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code doublePipes}
	 * labeled alternative in {@link DorisParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterDoublePipes(DorisParser.DoublePipesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code doublePipes}
	 * labeled alternative in {@link DorisParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitDoublePipes(DorisParser.DoublePipesContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#rowConstructor}.
	 * @param ctx the parse tree
	 */
	void enterRowConstructor(DorisParser.RowConstructorContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#rowConstructor}.
	 * @param ctx the parse tree
	 */
	void exitRowConstructor(DorisParser.RowConstructorContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#rowConstructorItem}.
	 * @param ctx the parse tree
	 */
	void enterRowConstructorItem(DorisParser.RowConstructorItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#rowConstructorItem}.
	 * @param ctx the parse tree
	 */
	void exitRowConstructorItem(DorisParser.RowConstructorItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterPredicate(DorisParser.PredicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitPredicate(DorisParser.PredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link DorisParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterValueExpressionDefault(DorisParser.ValueExpressionDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link DorisParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitValueExpressionDefault(DorisParser.ValueExpressionDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link DorisParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterComparison(DorisParser.ComparisonContext ctx);
	/**
	 * Exit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link DorisParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitComparison(DorisParser.ComparisonContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link DorisParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticBinary(DorisParser.ArithmeticBinaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link DorisParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticBinary(DorisParser.ArithmeticBinaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link DorisParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticUnary(DorisParser.ArithmeticUnaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link DorisParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticUnary(DorisParser.ArithmeticUnaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterDereference(DorisParser.DereferenceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitDereference(DorisParser.DereferenceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code currentDate}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCurrentDate(DorisParser.CurrentDateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code currentDate}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCurrentDate(DorisParser.CurrentDateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cast}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCast(DorisParser.CastContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCast(DorisParser.CastContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterParenthesizedExpression(DorisParser.ParenthesizedExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitParenthesizedExpression(DorisParser.ParenthesizedExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code userVariable}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterUserVariable(DorisParser.UserVariableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code userVariable}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitUserVariable(DorisParser.UserVariableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code elementAt}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterElementAt(DorisParser.ElementAtContext ctx);
	/**
	 * Exit a parse tree produced by the {@code elementAt}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitElementAt(DorisParser.ElementAtContext ctx);
	/**
	 * Enter a parse tree produced by the {@code localTimestamp}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterLocalTimestamp(DorisParser.LocalTimestampContext ctx);
	/**
	 * Exit a parse tree produced by the {@code localTimestamp}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitLocalTimestamp(DorisParser.LocalTimestampContext ctx);
	/**
	 * Enter a parse tree produced by the {@code charFunction}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCharFunction(DorisParser.CharFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code charFunction}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCharFunction(DorisParser.CharFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterIntervalLiteral(DorisParser.IntervalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitIntervalLiteral(DorisParser.IntervalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSimpleCase(DorisParser.SimpleCaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSimpleCase(DorisParser.SimpleCaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterColumnReference(DorisParser.ColumnReferenceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitColumnReference(DorisParser.ColumnReferenceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code star}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterStar(DorisParser.StarContext ctx);
	/**
	 * Exit a parse tree produced by the {@code star}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitStar(DorisParser.StarContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sessionUser}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSessionUser(DorisParser.SessionUserContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sessionUser}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSessionUser(DorisParser.SessionUserContext ctx);
	/**
	 * Enter a parse tree produced by the {@code convertType}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterConvertType(DorisParser.ConvertTypeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code convertType}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitConvertType(DorisParser.ConvertTypeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code convertCharSet}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterConvertCharSet(DorisParser.ConvertCharSetContext ctx);
	/**
	 * Exit a parse tree produced by the {@code convertCharSet}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitConvertCharSet(DorisParser.ConvertCharSetContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubqueryExpression(DorisParser.SubqueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubqueryExpression(DorisParser.SubqueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code encryptKey}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterEncryptKey(DorisParser.EncryptKeyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code encryptKey}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitEncryptKey(DorisParser.EncryptKeyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code currentTime}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCurrentTime(DorisParser.CurrentTimeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code currentTime}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCurrentTime(DorisParser.CurrentTimeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code localTime}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterLocalTime(DorisParser.LocalTimeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code localTime}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitLocalTime(DorisParser.LocalTimeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code systemVariable}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSystemVariable(DorisParser.SystemVariableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code systemVariable}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSystemVariable(DorisParser.SystemVariableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code collate}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCollate(DorisParser.CollateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code collate}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCollate(DorisParser.CollateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code currentUser}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCurrentUser(DorisParser.CurrentUserContext ctx);
	/**
	 * Exit a parse tree produced by the {@code currentUser}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCurrentUser(DorisParser.CurrentUserContext ctx);
	/**
	 * Enter a parse tree produced by the {@code constantDefault}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterConstantDefault(DorisParser.ConstantDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code constantDefault}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitConstantDefault(DorisParser.ConstantDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code extract}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterExtract(DorisParser.ExtractContext ctx);
	/**
	 * Exit a parse tree produced by the {@code extract}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitExtract(DorisParser.ExtractContext ctx);
	/**
	 * Enter a parse tree produced by the {@code currentTimestamp}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCurrentTimestamp(DorisParser.CurrentTimestampContext ctx);
	/**
	 * Exit a parse tree produced by the {@code currentTimestamp}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCurrentTimestamp(DorisParser.CurrentTimestampContext ctx);
	/**
	 * Enter a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterFunctionCall(DorisParser.FunctionCallContext ctx);
	/**
	 * Exit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitFunctionCall(DorisParser.FunctionCallContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arraySlice}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterArraySlice(DorisParser.ArraySliceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arraySlice}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitArraySlice(DorisParser.ArraySliceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSearchedCase(DorisParser.SearchedCaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link DorisParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSearchedCase(DorisParser.SearchedCaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code except}
	 * labeled alternative in {@link DorisParser#exceptOrReplace}.
	 * @param ctx the parse tree
	 */
	void enterExcept(DorisParser.ExceptContext ctx);
	/**
	 * Exit a parse tree produced by the {@code except}
	 * labeled alternative in {@link DorisParser#exceptOrReplace}.
	 * @param ctx the parse tree
	 */
	void exitExcept(DorisParser.ExceptContext ctx);
	/**
	 * Enter a parse tree produced by the {@code replace}
	 * labeled alternative in {@link DorisParser#exceptOrReplace}.
	 * @param ctx the parse tree
	 */
	void enterReplace(DorisParser.ReplaceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code replace}
	 * labeled alternative in {@link DorisParser#exceptOrReplace}.
	 * @param ctx the parse tree
	 */
	void exitReplace(DorisParser.ReplaceContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#castDataType}.
	 * @param ctx the parse tree
	 */
	void enterCastDataType(DorisParser.CastDataTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#castDataType}.
	 * @param ctx the parse tree
	 */
	void exitCastDataType(DorisParser.CastDataTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#functionCallExpression}.
	 * @param ctx the parse tree
	 */
	void enterFunctionCallExpression(DorisParser.FunctionCallExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#functionCallExpression}.
	 * @param ctx the parse tree
	 */
	void exitFunctionCallExpression(DorisParser.FunctionCallExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#functionIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterFunctionIdentifier(DorisParser.FunctionIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#functionIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitFunctionIdentifier(DorisParser.FunctionIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#functionNameIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterFunctionNameIdentifier(DorisParser.FunctionNameIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#functionNameIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitFunctionNameIdentifier(DorisParser.FunctionNameIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void enterWindowSpec(DorisParser.WindowSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void exitWindowSpec(DorisParser.WindowSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#windowFrame}.
	 * @param ctx the parse tree
	 */
	void enterWindowFrame(DorisParser.WindowFrameContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#windowFrame}.
	 * @param ctx the parse tree
	 */
	void exitWindowFrame(DorisParser.WindowFrameContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#frameUnits}.
	 * @param ctx the parse tree
	 */
	void enterFrameUnits(DorisParser.FrameUnitsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#frameUnits}.
	 * @param ctx the parse tree
	 */
	void exitFrameUnits(DorisParser.FrameUnitsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#frameBoundary}.
	 * @param ctx the parse tree
	 */
	void enterFrameBoundary(DorisParser.FrameBoundaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#frameBoundary}.
	 * @param ctx the parse tree
	 */
	void exitFrameBoundary(DorisParser.FrameBoundaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void enterQualifiedName(DorisParser.QualifiedNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void exitQualifiedName(DorisParser.QualifiedNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#specifiedPartition}.
	 * @param ctx the parse tree
	 */
	void enterSpecifiedPartition(DorisParser.SpecifiedPartitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#specifiedPartition}.
	 * @param ctx the parse tree
	 */
	void exitSpecifiedPartition(DorisParser.SpecifiedPartitionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterNullLiteral(DorisParser.NullLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitNullLiteral(DorisParser.NullLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterTypeConstructor(DorisParser.TypeConstructorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitTypeConstructor(DorisParser.TypeConstructorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterNumericLiteral(DorisParser.NumericLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitNumericLiteral(DorisParser.NumericLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterBooleanLiteral(DorisParser.BooleanLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitBooleanLiteral(DorisParser.BooleanLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterStringLiteral(DorisParser.StringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitStringLiteral(DorisParser.StringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arrayLiteral}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterArrayLiteral(DorisParser.ArrayLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arrayLiteral}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitArrayLiteral(DorisParser.ArrayLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code mapLiteral}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterMapLiteral(DorisParser.MapLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code mapLiteral}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitMapLiteral(DorisParser.MapLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code structLiteral}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterStructLiteral(DorisParser.StructLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code structLiteral}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitStructLiteral(DorisParser.StructLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code placeholder}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterPlaceholder(DorisParser.PlaceholderContext ctx);
	/**
	 * Exit a parse tree produced by the {@code placeholder}
	 * labeled alternative in {@link DorisParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitPlaceholder(DorisParser.PlaceholderContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void enterComparisonOperator(DorisParser.ComparisonOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void exitComparisonOperator(DorisParser.ComparisonOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void enterBooleanValue(DorisParser.BooleanValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void exitBooleanValue(DorisParser.BooleanValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#whenClause}.
	 * @param ctx the parse tree
	 */
	void enterWhenClause(DorisParser.WhenClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#whenClause}.
	 * @param ctx the parse tree
	 */
	void exitWhenClause(DorisParser.WhenClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#interval}.
	 * @param ctx the parse tree
	 */
	void enterInterval(DorisParser.IntervalContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#interval}.
	 * @param ctx the parse tree
	 */
	void exitInterval(DorisParser.IntervalContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#unitIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterUnitIdentifier(DorisParser.UnitIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#unitIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitUnitIdentifier(DorisParser.UnitIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#dataTypeWithNullable}.
	 * @param ctx the parse tree
	 */
	void enterDataTypeWithNullable(DorisParser.DataTypeWithNullableContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#dataTypeWithNullable}.
	 * @param ctx the parse tree
	 */
	void exitDataTypeWithNullable(DorisParser.DataTypeWithNullableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code complexDataType}
	 * labeled alternative in {@link DorisParser#dataType}.
	 * @param ctx the parse tree
	 */
	void enterComplexDataType(DorisParser.ComplexDataTypeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code complexDataType}
	 * labeled alternative in {@link DorisParser#dataType}.
	 * @param ctx the parse tree
	 */
	void exitComplexDataType(DorisParser.ComplexDataTypeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code variantPredefinedFields}
	 * labeled alternative in {@link DorisParser#dataType}.
	 * @param ctx the parse tree
	 */
	void enterVariantPredefinedFields(DorisParser.VariantPredefinedFieldsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code variantPredefinedFields}
	 * labeled alternative in {@link DorisParser#dataType}.
	 * @param ctx the parse tree
	 */
	void exitVariantPredefinedFields(DorisParser.VariantPredefinedFieldsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code aggStateDataType}
	 * labeled alternative in {@link DorisParser#dataType}.
	 * @param ctx the parse tree
	 */
	void enterAggStateDataType(DorisParser.AggStateDataTypeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code aggStateDataType}
	 * labeled alternative in {@link DorisParser#dataType}.
	 * @param ctx the parse tree
	 */
	void exitAggStateDataType(DorisParser.AggStateDataTypeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code primitiveDataType}
	 * labeled alternative in {@link DorisParser#dataType}.
	 * @param ctx the parse tree
	 */
	void enterPrimitiveDataType(DorisParser.PrimitiveDataTypeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code primitiveDataType}
	 * labeled alternative in {@link DorisParser#dataType}.
	 * @param ctx the parse tree
	 */
	void exitPrimitiveDataType(DorisParser.PrimitiveDataTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#primitiveColType}.
	 * @param ctx the parse tree
	 */
	void enterPrimitiveColType(DorisParser.PrimitiveColTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#primitiveColType}.
	 * @param ctx the parse tree
	 */
	void exitPrimitiveColType(DorisParser.PrimitiveColTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#complexColTypeList}.
	 * @param ctx the parse tree
	 */
	void enterComplexColTypeList(DorisParser.ComplexColTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#complexColTypeList}.
	 * @param ctx the parse tree
	 */
	void exitComplexColTypeList(DorisParser.ComplexColTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#complexColType}.
	 * @param ctx the parse tree
	 */
	void enterComplexColType(DorisParser.ComplexColTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#complexColType}.
	 * @param ctx the parse tree
	 */
	void exitComplexColType(DorisParser.ComplexColTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#variantSubColTypeList}.
	 * @param ctx the parse tree
	 */
	void enterVariantSubColTypeList(DorisParser.VariantSubColTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#variantSubColTypeList}.
	 * @param ctx the parse tree
	 */
	void exitVariantSubColTypeList(DorisParser.VariantSubColTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#variantSubColType}.
	 * @param ctx the parse tree
	 */
	void enterVariantSubColType(DorisParser.VariantSubColTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#variantSubColType}.
	 * @param ctx the parse tree
	 */
	void exitVariantSubColType(DorisParser.VariantSubColTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#variantSubColMatchType}.
	 * @param ctx the parse tree
	 */
	void enterVariantSubColMatchType(DorisParser.VariantSubColMatchTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#variantSubColMatchType}.
	 * @param ctx the parse tree
	 */
	void exitVariantSubColMatchType(DorisParser.VariantSubColMatchTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#commentSpec}.
	 * @param ctx the parse tree
	 */
	void enterCommentSpec(DorisParser.CommentSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#commentSpec}.
	 * @param ctx the parse tree
	 */
	void exitCommentSpec(DorisParser.CommentSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#sample}.
	 * @param ctx the parse tree
	 */
	void enterSample(DorisParser.SampleContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#sample}.
	 * @param ctx the parse tree
	 */
	void exitSample(DorisParser.SampleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sampleByPercentile}
	 * labeled alternative in {@link DorisParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void enterSampleByPercentile(DorisParser.SampleByPercentileContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sampleByPercentile}
	 * labeled alternative in {@link DorisParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void exitSampleByPercentile(DorisParser.SampleByPercentileContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sampleByRows}
	 * labeled alternative in {@link DorisParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void enterSampleByRows(DorisParser.SampleByRowsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sampleByRows}
	 * labeled alternative in {@link DorisParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void exitSampleByRows(DorisParser.SampleByRowsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#tableSnapshot}.
	 * @param ctx the parse tree
	 */
	void enterTableSnapshot(DorisParser.TableSnapshotContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#tableSnapshot}.
	 * @param ctx the parse tree
	 */
	void exitTableSnapshot(DorisParser.TableSnapshotContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#errorCapturingIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterErrorCapturingIdentifier(DorisParser.ErrorCapturingIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#errorCapturingIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitErrorCapturingIdentifier(DorisParser.ErrorCapturingIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code errorIdent}
	 * labeled alternative in {@link DorisParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 */
	void enterErrorIdent(DorisParser.ErrorIdentContext ctx);
	/**
	 * Exit a parse tree produced by the {@code errorIdent}
	 * labeled alternative in {@link DorisParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 */
	void exitErrorIdent(DorisParser.ErrorIdentContext ctx);
	/**
	 * Enter a parse tree produced by the {@code realIdent}
	 * labeled alternative in {@link DorisParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 */
	void enterRealIdent(DorisParser.RealIdentContext ctx);
	/**
	 * Exit a parse tree produced by the {@code realIdent}
	 * labeled alternative in {@link DorisParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 */
	void exitRealIdent(DorisParser.RealIdentContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(DorisParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(DorisParser.IdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link DorisParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterUnquotedIdentifier(DorisParser.UnquotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link DorisParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitUnquotedIdentifier(DorisParser.UnquotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link DorisParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterQuotedIdentifierAlternative(DorisParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link DorisParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitQuotedIdentifierAlternative(DorisParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterQuotedIdentifier(DorisParser.QuotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitQuotedIdentifier(DorisParser.QuotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link DorisParser#number}.
	 * @param ctx the parse tree
	 */
	void enterIntegerLiteral(DorisParser.IntegerLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link DorisParser#number}.
	 * @param ctx the parse tree
	 */
	void exitIntegerLiteral(DorisParser.IntegerLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link DorisParser#number}.
	 * @param ctx the parse tree
	 */
	void enterDecimalLiteral(DorisParser.DecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link DorisParser#number}.
	 * @param ctx the parse tree
	 */
	void exitDecimalLiteral(DorisParser.DecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link DorisParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void enterNonReserved(DorisParser.NonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link DorisParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void exitNonReserved(DorisParser.NonReservedContext ctx);
}