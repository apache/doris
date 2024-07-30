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

package org.apache.doris.nereids.parser;

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.ColumnNullableType;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.BuiltinAggregateFunctions;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.job.common.IntervalUnit;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVRefreshEnum.BuildMode;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshTrigger;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.mtmv.MTMVRefreshSchedule;
import org.apache.doris.mtmv.MTMVRefreshTriggerInfo;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.AddConstraintContext;
import org.apache.doris.nereids.DorisParser.AggClauseContext;
import org.apache.doris.nereids.DorisParser.AggStateDataTypeContext;
import org.apache.doris.nereids.DorisParser.AliasQueryContext;
import org.apache.doris.nereids.DorisParser.AliasedQueryContext;
import org.apache.doris.nereids.DorisParser.AlterMTMVContext;
import org.apache.doris.nereids.DorisParser.AlterStorageVaultContext;
import org.apache.doris.nereids.DorisParser.AlterViewContext;
import org.apache.doris.nereids.DorisParser.ArithmeticBinaryContext;
import org.apache.doris.nereids.DorisParser.ArithmeticUnaryContext;
import org.apache.doris.nereids.DorisParser.ArrayLiteralContext;
import org.apache.doris.nereids.DorisParser.ArrayRangeContext;
import org.apache.doris.nereids.DorisParser.ArraySliceContext;
import org.apache.doris.nereids.DorisParser.BitOperationContext;
import org.apache.doris.nereids.DorisParser.BooleanExpressionContext;
import org.apache.doris.nereids.DorisParser.BooleanLiteralContext;
import org.apache.doris.nereids.DorisParser.BracketDistributeTypeContext;
import org.apache.doris.nereids.DorisParser.BracketRelationHintContext;
import org.apache.doris.nereids.DorisParser.BuildModeContext;
import org.apache.doris.nereids.DorisParser.CallProcedureContext;
import org.apache.doris.nereids.DorisParser.CancelMTMVTaskContext;
import org.apache.doris.nereids.DorisParser.CastDataTypeContext;
import org.apache.doris.nereids.DorisParser.CollateContext;
import org.apache.doris.nereids.DorisParser.ColumnDefContext;
import org.apache.doris.nereids.DorisParser.ColumnDefsContext;
import org.apache.doris.nereids.DorisParser.ColumnReferenceContext;
import org.apache.doris.nereids.DorisParser.CommentDistributeTypeContext;
import org.apache.doris.nereids.DorisParser.CommentRelationHintContext;
import org.apache.doris.nereids.DorisParser.ComparisonContext;
import org.apache.doris.nereids.DorisParser.ComplexColTypeContext;
import org.apache.doris.nereids.DorisParser.ComplexColTypeListContext;
import org.apache.doris.nereids.DorisParser.ComplexDataTypeContext;
import org.apache.doris.nereids.DorisParser.ConstantContext;
import org.apache.doris.nereids.DorisParser.ConstantSeqContext;
import org.apache.doris.nereids.DorisParser.CreateMTMVContext;
import org.apache.doris.nereids.DorisParser.CreateProcedureContext;
import org.apache.doris.nereids.DorisParser.CreateRowPolicyContext;
import org.apache.doris.nereids.DorisParser.CreateTableContext;
import org.apache.doris.nereids.DorisParser.CreateTableLikeContext;
import org.apache.doris.nereids.DorisParser.CreateViewContext;
import org.apache.doris.nereids.DorisParser.CteContext;
import org.apache.doris.nereids.DorisParser.DataTypeWithNullableContext;
import org.apache.doris.nereids.DorisParser.DateCeilContext;
import org.apache.doris.nereids.DorisParser.DateFloorContext;
import org.apache.doris.nereids.DorisParser.Date_addContext;
import org.apache.doris.nereids.DorisParser.Date_subContext;
import org.apache.doris.nereids.DorisParser.DecimalLiteralContext;
import org.apache.doris.nereids.DorisParser.DeleteContext;
import org.apache.doris.nereids.DorisParser.DereferenceContext;
import org.apache.doris.nereids.DorisParser.DropCatalogRecycleBinContext;
import org.apache.doris.nereids.DorisParser.DropConstraintContext;
import org.apache.doris.nereids.DorisParser.DropMTMVContext;
import org.apache.doris.nereids.DorisParser.DropProcedureContext;
import org.apache.doris.nereids.DorisParser.ElementAtContext;
import org.apache.doris.nereids.DorisParser.ExistContext;
import org.apache.doris.nereids.DorisParser.ExplainContext;
import org.apache.doris.nereids.DorisParser.ExportContext;
import org.apache.doris.nereids.DorisParser.FixedPartitionDefContext;
import org.apache.doris.nereids.DorisParser.FromClauseContext;
import org.apache.doris.nereids.DorisParser.GroupingElementContext;
import org.apache.doris.nereids.DorisParser.GroupingSetContext;
import org.apache.doris.nereids.DorisParser.HavingClauseContext;
import org.apache.doris.nereids.DorisParser.HintAssignmentContext;
import org.apache.doris.nereids.DorisParser.HintStatementContext;
import org.apache.doris.nereids.DorisParser.IdentifierContext;
import org.apache.doris.nereids.DorisParser.IdentifierListContext;
import org.apache.doris.nereids.DorisParser.IdentifierOrTextContext;
import org.apache.doris.nereids.DorisParser.IdentifierSeqContext;
import org.apache.doris.nereids.DorisParser.InPartitionDefContext;
import org.apache.doris.nereids.DorisParser.IndexDefContext;
import org.apache.doris.nereids.DorisParser.IndexDefsContext;
import org.apache.doris.nereids.DorisParser.InlineTableContext;
import org.apache.doris.nereids.DorisParser.InsertTableContext;
import org.apache.doris.nereids.DorisParser.IntegerLiteralContext;
import org.apache.doris.nereids.DorisParser.IntervalContext;
import org.apache.doris.nereids.DorisParser.Is_not_null_predContext;
import org.apache.doris.nereids.DorisParser.IsnullContext;
import org.apache.doris.nereids.DorisParser.JoinCriteriaContext;
import org.apache.doris.nereids.DorisParser.JoinRelationContext;
import org.apache.doris.nereids.DorisParser.LambdaExpressionContext;
import org.apache.doris.nereids.DorisParser.LateralViewContext;
import org.apache.doris.nereids.DorisParser.LessThanPartitionDefContext;
import org.apache.doris.nereids.DorisParser.LimitClauseContext;
import org.apache.doris.nereids.DorisParser.LogicalBinaryContext;
import org.apache.doris.nereids.DorisParser.LogicalNotContext;
import org.apache.doris.nereids.DorisParser.MapLiteralContext;
import org.apache.doris.nereids.DorisParser.MultiStatementsContext;
import org.apache.doris.nereids.DorisParser.MultipartIdentifierContext;
import org.apache.doris.nereids.DorisParser.MvPartitionContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionSeqContext;
import org.apache.doris.nereids.DorisParser.NullLiteralContext;
import org.apache.doris.nereids.DorisParser.OutFileClauseContext;
import org.apache.doris.nereids.DorisParser.ParenthesizedExpressionContext;
import org.apache.doris.nereids.DorisParser.PartitionSpecContext;
import org.apache.doris.nereids.DorisParser.PartitionValueDefContext;
import org.apache.doris.nereids.DorisParser.PartitionsDefContext;
import org.apache.doris.nereids.DorisParser.PauseMTMVContext;
import org.apache.doris.nereids.DorisParser.PlanTypeContext;
import org.apache.doris.nereids.DorisParser.PredicateContext;
import org.apache.doris.nereids.DorisParser.PredicatedContext;
import org.apache.doris.nereids.DorisParser.PrimitiveDataTypeContext;
import org.apache.doris.nereids.DorisParser.PropertyClauseContext;
import org.apache.doris.nereids.DorisParser.PropertyItemContext;
import org.apache.doris.nereids.DorisParser.PropertyItemListContext;
import org.apache.doris.nereids.DorisParser.PropertyKeyContext;
import org.apache.doris.nereids.DorisParser.PropertyValueContext;
import org.apache.doris.nereids.DorisParser.QualifiedNameContext;
import org.apache.doris.nereids.DorisParser.QueryContext;
import org.apache.doris.nereids.DorisParser.QueryOrganizationContext;
import org.apache.doris.nereids.DorisParser.QueryTermContext;
import org.apache.doris.nereids.DorisParser.RefreshMTMVContext;
import org.apache.doris.nereids.DorisParser.RefreshMethodContext;
import org.apache.doris.nereids.DorisParser.RefreshScheduleContext;
import org.apache.doris.nereids.DorisParser.RefreshTriggerContext;
import org.apache.doris.nereids.DorisParser.RegularQuerySpecificationContext;
import org.apache.doris.nereids.DorisParser.RelationContext;
import org.apache.doris.nereids.DorisParser.ResumeMTMVContext;
import org.apache.doris.nereids.DorisParser.RollupDefContext;
import org.apache.doris.nereids.DorisParser.RollupDefsContext;
import org.apache.doris.nereids.DorisParser.RowConstructorContext;
import org.apache.doris.nereids.DorisParser.RowConstructorItemContext;
import org.apache.doris.nereids.DorisParser.SampleByPercentileContext;
import org.apache.doris.nereids.DorisParser.SampleByRowsContext;
import org.apache.doris.nereids.DorisParser.SampleContext;
import org.apache.doris.nereids.DorisParser.SelectClauseContext;
import org.apache.doris.nereids.DorisParser.SelectColumnClauseContext;
import org.apache.doris.nereids.DorisParser.SelectHintContext;
import org.apache.doris.nereids.DorisParser.SetOperationContext;
import org.apache.doris.nereids.DorisParser.ShowConstraintContext;
import org.apache.doris.nereids.DorisParser.ShowCreateMTMVContext;
import org.apache.doris.nereids.DorisParser.ShowCreateProcedureContext;
import org.apache.doris.nereids.DorisParser.ShowProcedureStatusContext;
import org.apache.doris.nereids.DorisParser.SimpleColumnDefContext;
import org.apache.doris.nereids.DorisParser.SimpleColumnDefsContext;
import org.apache.doris.nereids.DorisParser.SingleStatementContext;
import org.apache.doris.nereids.DorisParser.SortClauseContext;
import org.apache.doris.nereids.DorisParser.SortItemContext;
import org.apache.doris.nereids.DorisParser.StarContext;
import org.apache.doris.nereids.DorisParser.StatementDefaultContext;
import org.apache.doris.nereids.DorisParser.StepPartitionDefContext;
import org.apache.doris.nereids.DorisParser.StringLiteralContext;
import org.apache.doris.nereids.DorisParser.StructLiteralContext;
import org.apache.doris.nereids.DorisParser.SubqueryContext;
import org.apache.doris.nereids.DorisParser.SubqueryExpressionContext;
import org.apache.doris.nereids.DorisParser.SystemVariableContext;
import org.apache.doris.nereids.DorisParser.TableAliasContext;
import org.apache.doris.nereids.DorisParser.TableNameContext;
import org.apache.doris.nereids.DorisParser.TableValuedFunctionContext;
import org.apache.doris.nereids.DorisParser.TimestampaddContext;
import org.apache.doris.nereids.DorisParser.TimestampdiffContext;
import org.apache.doris.nereids.DorisParser.TypeConstructorContext;
import org.apache.doris.nereids.DorisParser.UnitIdentifierContext;
import org.apache.doris.nereids.DorisParser.UnsupportedContext;
import org.apache.doris.nereids.DorisParser.UpdateAssignmentContext;
import org.apache.doris.nereids.DorisParser.UpdateAssignmentSeqContext;
import org.apache.doris.nereids.DorisParser.UpdateContext;
import org.apache.doris.nereids.DorisParser.UserIdentifyContext;
import org.apache.doris.nereids.DorisParser.UserVariableContext;
import org.apache.doris.nereids.DorisParser.WhereClauseContext;
import org.apache.doris.nereids.DorisParser.WindowFrameContext;
import org.apache.doris.nereids.DorisParser.WindowSpecContext;
import org.apache.doris.nereids.DorisParser.WithRemoteStorageSystemContext;
import org.apache.doris.nereids.DorisParserBaseVisitor;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.analyzer.UnboundVariable;
import org.apache.doris.nereids.analyzer.UnboundVariable.VariableType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintLeading;
import org.apache.doris.nereids.properties.SelectHintOrdered;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.properties.SelectHintUseCboRule;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.BitAnd;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.BitOr;
import org.apache.doris.nereids.trees.expressions.BitXor;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.DefaultValueSlot;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.ListQuery;
import org.apache.doris.nereids.trees.expressions.MatchAll;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.MatchPhrase;
import org.apache.doris.nereids.trees.expressions.MatchPhraseEdge;
import org.apache.doris.nereids.trees.expressions.MatchPhrasePrefix;
import org.apache.doris.nereids.trees.expressions.MatchRegexp;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.Regexp;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRange;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeDayUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeHourUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeMinuteUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeMonthUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeSecondUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeWeekUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayRangeYearUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraySlice;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Char;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConvertTo;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentTime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentUser;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncryptKeyRef;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HourCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HourFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinuteCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinuteFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Now;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeekCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeekFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeeksAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeeksDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeeksSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Xor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsSub;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Interval;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.commands.AddConstraintCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterStorageVaultCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterViewCommand;
import org.apache.doris.nereids.trees.plans.commands.CallCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelMTMVTaskCommand;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.Constraint;
import org.apache.doris.nereids.trees.plans.commands.CreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.CreatePolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateProcedureCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableLikeCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateViewCommand;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromCommand;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromUsingCommand;
import org.apache.doris.nereids.trees.plans.commands.DropCatalogRecycleBinCommand;
import org.apache.doris.nereids.trees.plans.commands.DropCatalogRecycleBinCommand.IdType;
import org.apache.doris.nereids.trees.plans.commands.DropConstraintCommand;
import org.apache.doris.nereids.trees.plans.commands.DropMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.DropProcedureCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.ExportCommand;
import org.apache.doris.nereids.trees.plans.commands.LoadCommand;
import org.apache.doris.nereids.trees.plans.commands.PauseMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.RefreshMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.ResumeMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowConstraintsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateProcedureCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowProcedureStatusCommand;
import org.apache.doris.nereids.trees.plans.commands.UnsupportedCommand;
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;
import org.apache.doris.nereids.trees.plans.commands.info.AlterMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.AlterMTMVPropertyInfo;
import org.apache.doris.nereids.trees.plans.commands.info.AlterMTMVRefreshInfo;
import org.apache.doris.nereids.trees.plans.commands.info.AlterMTMVRenameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.AlterMTMVReplaceInfo;
import org.apache.doris.nereids.trees.plans.commands.info.AlterViewInfo;
import org.apache.doris.nereids.trees.plans.commands.info.BulkLoadDataDesc;
import org.apache.doris.nereids.trees.plans.commands.info.BulkStorageDesc;
import org.apache.doris.nereids.trees.plans.commands.info.CancelMTMVTaskInfo;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CreateMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableLikeInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateViewInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.info.DefaultValue;
import org.apache.doris.nereids.trees.plans.commands.info.DistributionDescriptor;
import org.apache.doris.nereids.trees.plans.commands.info.DropMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.FixedRangePartition;
import org.apache.doris.nereids.trees.plans.commands.info.FuncNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.GeneratedColumnDesc;
import org.apache.doris.nereids.trees.plans.commands.info.InPartition;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.LessThanPartition;
import org.apache.doris.nereids.trees.plans.commands.info.MTMVPartitionDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionDefinition.MaxValue;
import org.apache.doris.nereids.trees.plans.commands.info.PauseMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.ResumeMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RollupDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.ShowCreateMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.SimpleColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.StepPartition;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.commands.insert.BatchInsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalInlineTable;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.UsingJoin;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.policy.FilterType;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Build a logical plan tree with unbounded nodes.
 */
@SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "OptionalGetWithoutIsPresent"})
public class LogicalPlanBuilder extends DorisParserBaseVisitor<Object> {
    // Sort the parameters with token position to keep the order with original placeholders
    // in prepared statement.Otherwise, the order maybe broken
    private final Map<Token, Placeholder> tokenPosToParameters = Maps.newTreeMap((pos1, pos2) -> {
        int line = pos1.getLine() - pos2.getLine();
        if (line != 0) {
            return line;
        }
        return pos1.getCharPositionInLine() - pos2.getCharPositionInLine();
    });

    @SuppressWarnings("unchecked")
    protected <T> T typedVisit(ParseTree ctx) {
        return (T) ctx.accept(this);
    }

    /**
     * Override the default behavior for all visit methods. This will only return a non-null result
     * when the context has only one child. This is done because there is no generic method to
     * combine the results of the context children. In all other cases null is returned.
     */
    @Override
    public Object visitChildren(RuleNode node) {
        if (node.getChildCount() == 1) {
            return node.getChild(0).accept(this);
        } else {
            return null;
        }
    }

    @Override
    public LogicalPlan visitSingleStatement(SingleStatementContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> (LogicalPlan) visit(ctx.statement()));
    }

    @Override
    public LogicalPlan visitStatementDefault(StatementDefaultContext ctx) {
        LogicalPlan plan = plan(ctx.query());
        if (ctx.outFileClause() != null) {
            plan = withOutFile(plan, ctx.outFileClause());
        } else {
            plan = new UnboundResultSink<>(plan);
        }
        return withExplain(plan, ctx.explain());
    }

    @Override
    public LogicalPlan visitInsertTable(InsertTableContext ctx) {
        boolean isOverwrite = ctx.INTO() == null;
        ImmutableList.Builder<String> tableName = ImmutableList.builder();
        if (null != ctx.tableName) {
            List<String> nameParts = visitMultipartIdentifier(ctx.tableName);
            tableName.addAll(nameParts);
        } else if (null != ctx.tableId) {
            // process group commit insert table command send by be
            TableName name = Env.getCurrentEnv().getCurrentCatalog()
                    .getTableNameByTableId(Long.valueOf(ctx.tableId.getText()));
            tableName.add(name.getDb());
            tableName.add(name.getTbl());
        } else {
            throw new ParseException("tableName and tableId cannot both be null");
        }
        Optional<String> labelName = ctx.labelName == null ? Optional.empty() : Optional.of(ctx.labelName.getText());
        List<String> colNames = ctx.cols == null ? ImmutableList.of() : visitIdentifierList(ctx.cols);
        // TODO visit partitionSpecCtx
        LogicalPlan plan = visitQuery(ctx.query());
        // partitionSpec may be NULL. means auto detect partition. only available when IOT
        Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(ctx.partitionSpec());
        boolean isAutoDetect = partitionSpec.second == null;
        LogicalSink<?> sink = UnboundTableSinkCreator.createUnboundTableSinkMaybeOverwrite(
                tableName.build(),
                colNames,
                ImmutableList.of(), // hints
                partitionSpec.first, // isTemp
                partitionSpec.second, // partition names
                isAutoDetect,
                isOverwrite,
                ConnectContext.get().getSessionVariable().isEnableUniqueKeyPartialUpdate(),
                DMLCommandType.INSERT,
                plan);
        Optional<LogicalPlan> cte = Optional.empty();
        if (ctx.cte() != null) {
            cte = Optional.ofNullable(withCte(plan, ctx.cte()));
        }
        LogicalPlan command;
        if (isOverwrite) {
            command = new InsertOverwriteTableCommand(sink, labelName, cte);
        } else {
            if (ConnectContext.get() != null && ConnectContext.get().isTxnModel()
                    && sink.child() instanceof LogicalInlineTable) {
                // FIXME: In legacy, the `insert into select 1` is handled as `insert into values`.
                //  In nereids, the original way is throw an AnalysisException and fallback to legacy.
                //  Now handle it as `insert into select`(a separate load job), should fix it as the legacy.
                command = new BatchInsertIntoTableCommand(sink);
            } else {
                command = new InsertIntoTableCommand(sink, labelName, Optional.empty(), cte);
            }
        }
        return withExplain(command, ctx.explain());
    }

    /**
     * return a pair, first will be true if partitions is temp partition, select is a list to present partition list.
     */
    @Override
    public Pair<Boolean, List<String>> visitPartitionSpec(PartitionSpecContext ctx) {
        List<String> partitions = ImmutableList.of();
        boolean temporaryPartition = false;
        if (ctx != null) {
            temporaryPartition = ctx.TEMPORARY() != null;
            if (ctx.ASTERISK() != null) {
                partitions = null;
            } else if (ctx.partition != null) {
                partitions = ImmutableList.of(ctx.partition.getText());
            } else {
                partitions = visitIdentifierList(ctx.partitions);
            }
        }
        return Pair.of(temporaryPartition, partitions);
    }

    @Override
    public CreateMTMVCommand visitCreateMTMV(CreateMTMVContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);

        BuildMode buildMode = visitBuildMode(ctx.buildMode());
        RefreshMethod refreshMethod = visitRefreshMethod(ctx.refreshMethod());
        MTMVRefreshTriggerInfo refreshTriggerInfo = visitRefreshTrigger(ctx.refreshTrigger());
        LogicalPlan logicalPlan = visitQuery(ctx.query());
        String querySql = getOriginSql(ctx.query());

        int bucketNum = FeConstants.default_bucket_num;
        if (ctx.INTEGER_VALUE() != null) {
            bucketNum = Integer.parseInt(ctx.INTEGER_VALUE().getText());
        }
        DistributionDescriptor desc = null;
        if (ctx.HASH() != null) {
            desc = new DistributionDescriptor(true, ctx.AUTO() != null, bucketNum,
                    visitIdentifierList(ctx.hashKeys));
        } else if (ctx.RANDOM() != null) {
            desc = new DistributionDescriptor(false, ctx.AUTO() != null, bucketNum, null);
        }

        Map<String, String> properties = ctx.propertyClause() != null
                ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();
        String comment = ctx.STRING_LITERAL() == null ? "" : LogicalPlanBuilderAssistant.escapeBackSlash(
                ctx.STRING_LITERAL().getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1));

        return new CreateMTMVCommand(new CreateMTMVInfo(ctx.EXISTS() != null, new TableNameInfo(nameParts),
                ctx.keys != null ? visitIdentifierList(ctx.keys) : ImmutableList.of(),
                comment,
                desc, properties, logicalPlan, querySql,
                new MTMVRefreshInfo(buildMode, refreshMethod, refreshTriggerInfo),
                ctx.cols == null ? Lists.newArrayList() : visitSimpleColumnDefs(ctx.cols),
                visitMTMVPartitionInfo(ctx.mvPartition())
        ));
    }

    /**
     * get MTMVPartitionDefinition
     *
     * @param ctx MvPartitionContext
     * @return MTMVPartitionDefinition
     */
    public MTMVPartitionDefinition visitMTMVPartitionInfo(MvPartitionContext ctx) {
        MTMVPartitionDefinition mtmvPartitionDefinition = new MTMVPartitionDefinition();
        if (ctx == null) {
            mtmvPartitionDefinition.setPartitionType(MTMVPartitionType.SELF_MANAGE);
        } else if (ctx.partitionKey != null) {
            mtmvPartitionDefinition.setPartitionType(MTMVPartitionType.FOLLOW_BASE_TABLE);
            mtmvPartitionDefinition.setPartitionCol(ctx.partitionKey.getText());
        } else {
            mtmvPartitionDefinition.setPartitionType(MTMVPartitionType.EXPR);
            Expression functionCallExpression = visitFunctionCallExpression(ctx.partitionExpr);
            mtmvPartitionDefinition.setFunctionCallExpression(functionCallExpression);
        }
        return mtmvPartitionDefinition;
    }

    @Override
    public List<SimpleColumnDefinition> visitSimpleColumnDefs(SimpleColumnDefsContext ctx) {
        if (ctx == null) {
            return null;
        }
        return ctx.cols.stream()
                .map(this::visitSimpleColumnDef)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public SimpleColumnDefinition visitSimpleColumnDef(SimpleColumnDefContext ctx) {
        String comment = ctx.STRING_LITERAL() == null ? "" : LogicalPlanBuilderAssistant.escapeBackSlash(
                ctx.STRING_LITERAL().getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1));
        return new SimpleColumnDefinition(ctx.colName.getText().toLowerCase(),
                comment);
    }

    /**
     * get originSql
     *
     * @param ctx context
     * @return originSql
     */
    public String getOriginSql(ParserRuleContext ctx) {
        int startIndex = ctx.start.getStartIndex();
        int stopIndex = ctx.stop.getStopIndex();
        org.antlr.v4.runtime.misc.Interval interval = new org.antlr.v4.runtime.misc.Interval(startIndex, stopIndex);
        return ctx.start.getInputStream().getText(interval);
    }

    @Override
    public MTMVRefreshTriggerInfo visitRefreshTrigger(RefreshTriggerContext ctx) {
        if (ctx == null) {
            return new MTMVRefreshTriggerInfo(RefreshTrigger.MANUAL);
        }
        if (ctx.MANUAL() != null) {
            return new MTMVRefreshTriggerInfo(RefreshTrigger.MANUAL);
        }
        if (ctx.COMMIT() != null) {
            return new MTMVRefreshTriggerInfo(RefreshTrigger.COMMIT);
        }
        if (ctx.SCHEDULE() != null) {
            return new MTMVRefreshTriggerInfo(RefreshTrigger.SCHEDULE, visitRefreshSchedule(ctx.refreshSchedule()));
        }
        return new MTMVRefreshTriggerInfo(RefreshTrigger.MANUAL);
    }

    @Override
    public MTMVRefreshSchedule visitRefreshSchedule(RefreshScheduleContext ctx) {
        int interval = Integer.parseInt(ctx.INTEGER_VALUE().getText());
        String startTime = ctx.STARTS() == null ? null
                : ctx.STRING_LITERAL().getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1);
        IntervalUnit unit = visitMvRefreshUnit(ctx.refreshUnit);
        return new MTMVRefreshSchedule(startTime, interval, unit);
    }

    /**
     * get IntervalUnit,only enable_job_schedule_second_for_test is true, can use second
     *
     * @param ctx ctx
     * @return IntervalUnit
     */
    public IntervalUnit visitMvRefreshUnit(IdentifierContext ctx) {
        IntervalUnit intervalUnit = IntervalUnit.fromString(ctx.getText().toUpperCase());
        if (null == intervalUnit) {
            throw new AnalysisException("interval time unit can not be " + ctx.getText());
        }
        if (intervalUnit.equals(IntervalUnit.SECOND)
                && !Config.enable_job_schedule_second_for_test) {
            throw new AnalysisException("interval time unit can not be second");
        }
        return intervalUnit;
    }

    @Override
    public RefreshMethod visitRefreshMethod(RefreshMethodContext ctx) {
        if (ctx == null) {
            return RefreshMethod.AUTO;
        }
        return RefreshMethod.valueOf(ctx.getText().toUpperCase());
    }

    @Override
    public BuildMode visitBuildMode(BuildModeContext ctx) {
        if (ctx == null) {
            return BuildMode.IMMEDIATE;
        }
        if (ctx.DEFERRED() != null) {
            return BuildMode.DEFERRED;
        } else if (ctx.IMMEDIATE() != null) {
            return BuildMode.IMMEDIATE;
        }
        return BuildMode.IMMEDIATE;
    }

    @Override
    public RefreshMTMVCommand visitRefreshMTMV(RefreshMTMVContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        List<String> partitions = ImmutableList.of();
        if (ctx.partitionSpec() != null) {
            if (ctx.partitionSpec().TEMPORARY() != null) {
                throw new AnalysisException("Not allowed to specify TEMPORARY ");
            }
            if (ctx.partitionSpec().partition != null) {
                partitions = ImmutableList.of(ctx.partitionSpec().partition.getText());
            } else {
                partitions = visitIdentifierList(ctx.partitionSpec().partitions);
            }
        }
        return new RefreshMTMVCommand(new RefreshMTMVInfo(new TableNameInfo(nameParts),
                partitions, ctx.COMPLETE() != null));
    }

    @Override
    public DropMTMVCommand visitDropMTMV(DropMTMVContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        return new DropMTMVCommand(new DropMTMVInfo(new TableNameInfo(nameParts), ctx.EXISTS() != null));
    }

    @Override
    public PauseMTMVCommand visitPauseMTMV(PauseMTMVContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        return new PauseMTMVCommand(new PauseMTMVInfo(new TableNameInfo(nameParts)));
    }

    @Override
    public ResumeMTMVCommand visitResumeMTMV(ResumeMTMVContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        return new ResumeMTMVCommand(new ResumeMTMVInfo(new TableNameInfo(nameParts)));
    }

    @Override
    public ShowCreateMTMVCommand visitShowCreateMTMV(ShowCreateMTMVContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        return new ShowCreateMTMVCommand(new ShowCreateMTMVInfo(new TableNameInfo(nameParts)));
    }

    @Override
    public CancelMTMVTaskCommand visitCancelMTMVTask(CancelMTMVTaskContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        long taskId = Long.parseLong(ctx.taskId.getText());
        return new CancelMTMVTaskCommand(new CancelMTMVTaskInfo(new TableNameInfo(nameParts), taskId));
    }

    @Override
    public AlterMTMVCommand visitAlterMTMV(AlterMTMVContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.mvName);
        TableNameInfo mvName = new TableNameInfo(nameParts);
        AlterMTMVInfo alterMTMVInfo = null;
        if (ctx.RENAME() != null) {
            alterMTMVInfo = new AlterMTMVRenameInfo(mvName, ctx.newName.getText());
        } else if (ctx.REFRESH() != null) {
            MTMVRefreshInfo refreshInfo = new MTMVRefreshInfo();
            if (ctx.refreshMethod() != null) {
                refreshInfo.setRefreshMethod(visitRefreshMethod(ctx.refreshMethod()));
            }
            if (ctx.refreshTrigger() != null) {
                refreshInfo.setRefreshTriggerInfo(visitRefreshTrigger(ctx.refreshTrigger()));
            }
            alterMTMVInfo = new AlterMTMVRefreshInfo(mvName, refreshInfo);
        } else if (ctx.SET() != null) {
            alterMTMVInfo = new AlterMTMVPropertyInfo(mvName,
                    Maps.newHashMap(visitPropertyItemList(ctx.fileProperties)));
        } else if (ctx.REPLACE() != null) {
            String newName = ctx.newName.getText();
            Map<String, String> properties = ctx.propertyClause() != null
                    ? Maps.newHashMap(visitPropertyClause(ctx.propertyClause())) : Maps.newHashMap();
            alterMTMVInfo = new AlterMTMVReplaceInfo(mvName, newName, properties);
        }
        return new AlterMTMVCommand(alterMTMVInfo);
    }

    @Override
    public LogicalPlan visitAlterView(AlterViewContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        String querySql = getOriginSql(ctx.query());
        AlterViewInfo info = new AlterViewInfo(new TableNameInfo(nameParts), querySql,
                ctx.cols == null ? Lists.newArrayList() : visitSimpleColumnDefs(ctx.cols));
        return new AlterViewCommand(info);
    }

    @Override
    public LogicalPlan visitAlterStorageVault(AlterStorageVaultContext ctx) {
        List<String> nameParts = this.visitMultipartIdentifier(ctx.name);
        String vaultName = nameParts.get(0);
        Map<String, String> properties = this.visitPropertyClause(ctx.properties);
        return new AlterStorageVaultCommand(vaultName, properties);
    }

    @Override
    public LogicalPlan visitShowConstraint(ShowConstraintContext ctx) {
        List<String> parts = visitMultipartIdentifier(ctx.table);
        return new ShowConstraintsCommand(parts);
    }

    @Override
    public LogicalPlan visitAddConstraint(AddConstraintContext ctx) {
        List<String> parts = visitMultipartIdentifier(ctx.table);
        UnboundRelation curTable = new UnboundRelation(StatementScopeIdGenerator.newRelationId(), parts);
        ImmutableList<Slot> slots = visitIdentifierList(ctx.constraint().slots).stream()
                .map(UnboundSlot::new)
                .collect(ImmutableList.toImmutableList());
        Constraint constraint;
        if (ctx.constraint().UNIQUE() != null) {
            constraint = Constraint.newUniqueConstraint(curTable, slots);
        } else if (ctx.constraint().PRIMARY() != null) {
            constraint = Constraint.newPrimaryKeyConstraint(curTable, slots);
        } else if (ctx.constraint().FOREIGN() != null) {
            ImmutableList<Slot> referencedSlots = visitIdentifierList(ctx.constraint().referencedSlots).stream()
                    .map(UnboundSlot::new)
                    .collect(ImmutableList.toImmutableList());
            List<String> nameParts = visitMultipartIdentifier(ctx.constraint().referenceTable);
            LogicalPlan referenceTable = new UnboundRelation(StatementScopeIdGenerator.newRelationId(), nameParts);
            constraint = Constraint.newForeignKeyConstraint(curTable, slots, referenceTable, referencedSlots);
        } else {
            throw new AnalysisException("Unsupported constraint " + ctx.getText());
        }
        return new AddConstraintCommand(ctx.constraintName.getText().toLowerCase(), constraint);
    }

    @Override
    public LogicalPlan visitDropConstraint(DropConstraintContext ctx) {
        List<String> parts = visitMultipartIdentifier(ctx.table);
        UnboundRelation curTable = new UnboundRelation(StatementScopeIdGenerator.newRelationId(), parts);
        return new DropConstraintCommand(ctx.constraintName.getText().toLowerCase(), curTable);
    }

    @Override
    public LogicalPlan visitUpdate(UpdateContext ctx) {
        LogicalPlan query = LogicalPlanBuilderAssistant.withCheckPolicy(new UnboundRelation(
                StatementScopeIdGenerator.newRelationId(), visitMultipartIdentifier(ctx.tableName)));
        query = withTableAlias(query, ctx.tableAlias());
        if (ctx.fromClause() != null) {
            query = withRelations(query, ctx.fromClause().relations().relation());
        }
        query = withFilter(query, Optional.ofNullable(ctx.whereClause()));
        String tableAlias = null;
        if (ctx.tableAlias().strictIdentifier() != null) {
            tableAlias = ctx.tableAlias().getText();
        }
        Optional<LogicalPlan> cte = Optional.empty();
        if (ctx.cte() != null) {
            cte = Optional.ofNullable(withCte(query, ctx.cte()));
        }
        return withExplain(new UpdateCommand(visitMultipartIdentifier(ctx.tableName), tableAlias,
                visitUpdateAssignmentSeq(ctx.updateAssignmentSeq()), query, cte), ctx.explain());
    }

    @Override
    public LogicalPlan visitDelete(DeleteContext ctx) {
        List<String> tableName = visitMultipartIdentifier(ctx.tableName);
        Pair<Boolean, List<String>> partitionSpec = visitPartitionSpec(ctx.partitionSpec());
        // TODO: now dont support delete auto detect partition.
        if (partitionSpec == null) {
            throw new ParseException("Now don't support auto detect partitions in deleting", ctx);
        }
        LogicalPlan query = withTableAlias(LogicalPlanBuilderAssistant.withCheckPolicy(
                new UnboundRelation(StatementScopeIdGenerator.newRelationId(), tableName,
                        partitionSpec.second, partitionSpec.first)), ctx.tableAlias());
        String tableAlias = null;
        if (ctx.tableAlias().strictIdentifier() != null) {
            tableAlias = ctx.tableAlias().getText();
        }

        Command deleteCommand;
        if (ctx.USING() == null && ctx.cte() == null) {
            query = withFilter(query, Optional.ofNullable(ctx.whereClause()));
            deleteCommand = new DeleteFromCommand(tableName, tableAlias, partitionSpec.first,
                    partitionSpec.second, query);
        } else {
            // convert to insert into select
            query = withRelations(query, ctx.relations().relation());
            query = withFilter(query, Optional.ofNullable(ctx.whereClause()));
            Optional<LogicalPlan> cte = Optional.empty();
            if (ctx.cte() != null) {
                cte = Optional.ofNullable(withCte(query, ctx.cte()));
            }
            deleteCommand = new DeleteFromUsingCommand(tableName, tableAlias,
                    partitionSpec.first, partitionSpec.second, query, cte);
        }
        if (ctx.explain() != null) {
            return withExplain(deleteCommand, ctx.explain());
        } else {
            return deleteCommand;
        }
    }

    @Override
    public LogicalPlan visitExport(ExportContext ctx) {
        // TODO: replace old class name like ExportStmt, BrokerDesc, Expr with new nereid class name
        List<String> tableName = visitMultipartIdentifier(ctx.tableName);
        List<String> partitions = ctx.partition == null ? ImmutableList.of() : visitIdentifierList(ctx.partition);

        // handle path string
        String tmpPath = ctx.filePath.getText();
        String path = LogicalPlanBuilderAssistant.escapeBackSlash(tmpPath.substring(1, tmpPath.length() - 1));

        Optional<Expression> expr = Optional.empty();
        if (ctx.whereClause() != null) {
            expr = Optional.of(getExpression(ctx.whereClause().booleanExpression()));
        }

        Map<String, String> filePropertiesMap = ImmutableMap.of();
        if (ctx.propertyClause() != null) {
            filePropertiesMap = visitPropertyClause(ctx.propertyClause());
        }

        Optional<BrokerDesc> brokerDesc = Optional.empty();
        if (ctx.withRemoteStorageSystem() != null) {
            brokerDesc = Optional.ofNullable(visitWithRemoteStorageSystem(ctx.withRemoteStorageSystem()));
        }
        return new ExportCommand(tableName, partitions, expr, path, filePropertiesMap, brokerDesc);
    }

    @Override
    public Map<String, String> visitPropertyClause(PropertyClauseContext ctx) {
        return ctx == null ? ImmutableMap.of() : visitPropertyItemList(ctx.fileProperties);
    }

    @Override
    public Map<String, String> visitPropertyItemList(PropertyItemListContext ctx) {
        if (ctx == null || ctx.properties == null) {
            return ImmutableMap.of();
        }
        Builder<String, String> propertiesMap = ImmutableMap.builder();
        for (PropertyItemContext argument : ctx.properties) {
            String key = parsePropertyKey(argument.key);
            String value = parsePropertyValue(argument.value);
            propertiesMap.put(key, value);
        }
        return propertiesMap.build();
    }

    @Override
    public BrokerDesc visitWithRemoteStorageSystem(WithRemoteStorageSystemContext ctx) {
        BrokerDesc brokerDesc = null;

        Map<String, String> brokerPropertiesMap = visitPropertyItemList(ctx.brokerProperties);

        if (ctx.S3() != null) {
            brokerDesc = new BrokerDesc("S3", StorageBackend.StorageType.S3, brokerPropertiesMap);
        } else if (ctx.HDFS() != null) {
            brokerDesc = new BrokerDesc("HDFS", StorageBackend.StorageType.HDFS, brokerPropertiesMap);
        } else if (ctx.LOCAL() != null) {
            brokerDesc = new BrokerDesc("HDFS", StorageBackend.StorageType.LOCAL, brokerPropertiesMap);
        } else if (ctx.BROKER() != null) {
            brokerDesc = new BrokerDesc(visitIdentifierOrText(ctx.brokerName), brokerPropertiesMap);
        }
        return brokerDesc;
    }

    /**
     * Visit multi-statements.
     */
    @Override
    public List<Pair<LogicalPlan, StatementContext>> visitMultiStatements(MultiStatementsContext ctx) {
        List<Pair<LogicalPlan, StatementContext>> logicalPlans = Lists.newArrayList();
        for (org.apache.doris.nereids.DorisParser.StatementContext statement : ctx.statement()) {
            StatementContext statementContext = new StatementContext();
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext != null) {
                connectContext.setStatementContext(statementContext);
                statementContext.setConnectContext(connectContext);
            }
            logicalPlans.add(Pair.of(
                    ParserUtils.withOrigin(ctx, () -> (LogicalPlan) visit(statement)), statementContext));
            List<Placeholder> params = new ArrayList<>(tokenPosToParameters.values());
            statementContext.setPlaceholders(params);
            tokenPosToParameters.clear();
        }
        return logicalPlans;
    }

    /**
     * Visit load-statements.
     */
    @Override
    public LogicalPlan visitLoad(DorisParser.LoadContext ctx) {

        BulkStorageDesc bulkDesc = null;
        if (ctx.withRemoteStorageSystem() != null) {
            Map<String, String> bulkProperties =
                    new HashMap<>(visitPropertyItemList(ctx.withRemoteStorageSystem().brokerProperties));
            if (ctx.withRemoteStorageSystem().S3() != null) {
                bulkDesc = new BulkStorageDesc("S3", BulkStorageDesc.StorageType.S3, bulkProperties);
            } else if (ctx.withRemoteStorageSystem().HDFS() != null) {
                bulkDesc = new BulkStorageDesc("HDFS", BulkStorageDesc.StorageType.HDFS, bulkProperties);
            } else if (ctx.withRemoteStorageSystem().LOCAL() != null) {
                bulkDesc = new BulkStorageDesc("LOCAL_HDFS", BulkStorageDesc.StorageType.LOCAL, bulkProperties);
            } else if (ctx.withRemoteStorageSystem().BROKER() != null
                    && ctx.withRemoteStorageSystem().identifierOrText().getText() != null) {
                bulkDesc = new BulkStorageDesc(ctx.withRemoteStorageSystem().identifierOrText().getText(),
                        bulkProperties);
            }
        }
        ImmutableList.Builder<BulkLoadDataDesc> dataDescriptions = new ImmutableList.Builder<>();
        for (DorisParser.DataDescContext ddc : ctx.dataDescs) {
            List<String> tableName = RelationUtil.getQualifierName(ConnectContext.get(),
                    visitMultipartIdentifier(ddc.tableName));
            List<String> colNames = (ddc.columns == null ? ImmutableList.of() : visitIdentifierList(ddc.columns));
            List<String> columnsFromPath = (ddc.columnsFromPath == null ? ImmutableList.of()
                        : visitIdentifierList(ddc.columnsFromPath.identifierList()));
            List<String> partitions = ddc.partition == null ? ImmutableList.of() : visitIdentifierList(ddc.partition);
            // TODO: multi location
            List<String> multiFilePaths = new ArrayList<>();
            for (Token filePath : ddc.filePaths) {
                multiFilePaths.add(filePath.getText().substring(1, filePath.getText().length() - 1));
            }
            List<String> filePaths = ddc.filePath == null ? ImmutableList.of() : multiFilePaths;
            Map<String, Expression> colMappings;
            if (ddc.columnMapping == null) {
                colMappings = ImmutableMap.of();
            } else {
                colMappings = new HashMap<>();
                for (DorisParser.MappingExprContext mappingExpr : ddc.columnMapping.mappingSet) {
                    colMappings.put(mappingExpr.mappingCol.getText(), getExpression(mappingExpr.expression()));
                }
            }

            LoadTask.MergeType mergeType = ddc.mergeType() == null ? LoadTask.MergeType.APPEND
                        : LoadTask.MergeType.valueOf(ddc.mergeType().getText());

            Optional<String> fileFormat = ddc.format == null ? Optional.empty()
                    : Optional.of(visitIdentifierOrStringLiteral(ddc.format));
            Optional<String> separator = ddc.separator == null ? Optional.empty() : Optional.of(ddc.separator.getText()
                        .substring(1, ddc.separator.getText().length() - 1));
            Optional<String> comma = ddc.comma == null ? Optional.empty() : Optional.of(ddc.comma.getText()
                        .substring(1, ddc.comma.getText().length() - 1));
            Map<String, String> dataProperties = ddc.propertyClause() == null ? new HashMap<>()
                        : visitPropertyClause(ddc.propertyClause());
            dataDescriptions.add(new BulkLoadDataDesc(
                    tableName,
                    partitions,
                    filePaths,
                    colNames,
                    columnsFromPath,
                    colMappings,
                    new BulkLoadDataDesc.FileFormatDesc(separator, comma, fileFormat),
                    false,
                    ddc.preFilter == null ? Optional.empty() : Optional.of(getExpression(ddc.preFilter.expression())),
                    ddc.where == null ? Optional.empty() : Optional.of(getExpression(ddc.where.booleanExpression())),
                    mergeType,
                    ddc.deleteOn == null ? Optional.empty() : Optional.of(getExpression(ddc.deleteOn.expression())),
                    ddc.sequenceColumn == null ? Optional.empty()
                            : Optional.of(ddc.sequenceColumn.identifier().getText()), dataProperties));
        }
        String labelName = ctx.lableName.getText();
        Map<String, String> properties = visitPropertyItemList(ctx.properties);
        String commentSpec = ctx.commentSpec() == null ? "''" : ctx.commentSpec().STRING_LITERAL().getText();
        String comment =
                LogicalPlanBuilderAssistant.escapeBackSlash(commentSpec.substring(1, commentSpec.length() - 1));
        return new LoadCommand(labelName, dataDescriptions.build(), bulkDesc, properties, comment);
    }

    /* ********************************************************************************************
     * Plan parsing
     * ******************************************************************************************** */

    /**
     * process lateral view, add a {@link org.apache.doris.nereids.trees.plans.logical.LogicalGenerate} on plan.
     */
    protected LogicalPlan withGenerate(LogicalPlan plan, LateralViewContext ctx) {
        if (ctx.LATERAL() == null) {
            return plan;
        }
        String generateName = ctx.tableName.getText();
        // if later view explode map type, we need to add a project to convert map to struct
        String columnName = ctx.columnNames.get(0).getText();
        List<String> expandColumnNames = Lists.newArrayList();
        if (ctx.columnNames.size() > 1) {
            columnName = ConnectContext.get() != null
                    ? ConnectContext.get().getStatementContext().generateColumnName() : "expand_cols";
            expandColumnNames = ctx.columnNames.stream()
                    .map(RuleContext::getText).collect(ImmutableList.toImmutableList());
        }
        String functionName = ctx.functionName.getText();
        List<Expression> arguments = ctx.expression().stream()
                .<Expression>map(this::typedVisit)
                .collect(ImmutableList.toImmutableList());
        Function unboundFunction = new UnboundFunction(functionName, arguments);
        return new LogicalGenerate<>(ImmutableList.of(unboundFunction),
                ImmutableList.of(new UnboundSlot(generateName, columnName)), ImmutableList.of(expandColumnNames), plan);
    }

    /**
     * process CTE and store the results in a logical plan node LogicalCTE
     */
    private LogicalPlan withCte(LogicalPlan plan, CteContext ctx) {
        if (ctx == null) {
            return plan;
        }
        return new LogicalCTE<>((List) visit(ctx.aliasQuery(), LogicalSubQueryAlias.class), plan);
    }

    /**
     * process CTE's alias queries and column aliases
     */
    @Override
    public LogicalSubQueryAlias<Plan> visitAliasQuery(AliasQueryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            LogicalPlan queryPlan = plan(ctx.query());
            Optional<List<String>> columnNames = optionalVisit(ctx.columnAliases(), () ->
                    ctx.columnAliases().identifier().stream()
                    .map(RuleContext::getText)
                    .collect(ImmutableList.toImmutableList())
            );
            return new LogicalSubQueryAlias<>(ctx.identifier().getText(), columnNames, queryPlan);
        });
    }

    @Override
    public Command visitCreateRowPolicy(CreateRowPolicyContext ctx) {
        FilterType filterType = FilterType.of(ctx.type.getText());
        List<String> nameParts = visitMultipartIdentifier(ctx.table);
        return new CreatePolicyCommand(PolicyTypeEnum.ROW, ctx.name.getText(),
                ctx.EXISTS() != null, nameParts, Optional.of(filterType),
                ctx.user == null ? null : visitUserIdentify(ctx.user),
                ctx.roleName == null ? null : ctx.roleName.getText(),
                Optional.of(getExpression(ctx.booleanExpression())), ImmutableMap.of());
    }

    @Override
    public String visitIdentifierOrText(IdentifierOrTextContext ctx) {
        if (ctx.STRING_LITERAL() != null) {
            return ctx.STRING_LITERAL().getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1);
        } else {
            return ctx.errorCapturingIdentifier().getText();
        }
    }

    @Override
    public String visitIdentifierOrStringLiteral(DorisParser.IdentifierOrStringLiteralContext ctx) {
        if (ctx.STRING_LITERAL() != null) {
            return ctx.STRING_LITERAL().getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1);
        } else {
            return ctx.identifier().getText();
        }
    }

    @Override
    public UserIdentity visitUserIdentify(UserIdentifyContext ctx) {
        String user = visitIdentifierOrText(ctx.user);
        String host = null;
        if (ctx.host != null) {
            host = visitIdentifierOrText(ctx.host);
        }
        if (host == null) {
            host = "%";
        }
        boolean isDomain = ctx.LEFT_PAREN() != null;
        return new UserIdentity(user, host, isDomain);
    }

    @Override
    public LogicalPlan visitQuery(QueryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            // TODO: need to add withQueryResultClauses and withCTE
            LogicalPlan query = plan(ctx.queryTerm());
            query = withCte(query, ctx.cte());
            return withQueryOrganization(query, ctx.queryOrganization());
        });
    }

    @Override
    public LogicalPlan visitSetOperation(SetOperationContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {

            if (ctx.UNION() != null) {
                Qualifier qualifier = getQualifier(ctx);
                List<QueryTermContext> contexts = Lists.newArrayList(ctx.right);
                QueryTermContext current = ctx.left;
                while (true) {
                    if (current instanceof SetOperationContext
                            && getQualifier((SetOperationContext) current) == qualifier
                            && ((SetOperationContext) current).UNION() != null) {
                        contexts.add(((SetOperationContext) current).right);
                        current = ((SetOperationContext) current).left;
                    } else {
                        contexts.add(current);
                        break;
                    }
                }
                Collections.reverse(contexts);
                List<LogicalPlan> logicalPlans = contexts.stream().map(this::plan).collect(Collectors.toList());
                return reduceToLogicalPlanTree(0, logicalPlans.size() - 1, logicalPlans, qualifier);
            } else {
                LogicalPlan leftQuery = plan(ctx.left);
                LogicalPlan rightQuery = plan(ctx.right);
                Qualifier qualifier = getQualifier(ctx);

                List<Plan> newChildren = ImmutableList.of(leftQuery, rightQuery);
                LogicalPlan plan;
                if (ctx.UNION() != null) {
                    plan = new LogicalUnion(qualifier, newChildren);
                } else if (ctx.EXCEPT() != null || ctx.MINUS() != null) {
                    plan = new LogicalExcept(qualifier, newChildren);
                } else if (ctx.INTERSECT() != null) {
                    plan = new LogicalIntersect(qualifier, newChildren);
                } else {
                    throw new ParseException("not support", ctx);
                }
                return plan;
            }
        });
    }

    private Qualifier getQualifier(SetOperationContext ctx) {
        if (ctx.setQuantifier() == null || ctx.setQuantifier().DISTINCT() != null) {
            return Qualifier.DISTINCT;
        } else {
            return Qualifier.ALL;
        }
    }

    private static LogicalPlan logicalPlanCombiner(LogicalPlan left, LogicalPlan right, Qualifier qualifier) {
        return new LogicalUnion(qualifier, ImmutableList.of(left, right));
    }

    /**
     * construct avl union tree
     */
    public static LogicalPlan reduceToLogicalPlanTree(int low, int high,
            List<LogicalPlan> logicalPlans, Qualifier qualifier) {
        switch (high - low) {
            case 0:
                return logicalPlans.get(low);
            case 1:
                return logicalPlanCombiner(logicalPlans.get(low), logicalPlans.get(high), qualifier);
            default:
                int mid = low + (high - low) / 2;
                return logicalPlanCombiner(
                        reduceToLogicalPlanTree(low, mid, logicalPlans, qualifier),
                        reduceToLogicalPlanTree(mid + 1, high, logicalPlans, qualifier),
                        qualifier
                );
        }
    }

    @Override
    public LogicalPlan visitSubquery(SubqueryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> plan(ctx.query()));
    }

    @Override
    public LogicalPlan visitRegularQuerySpecification(RegularQuerySpecificationContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            SelectClauseContext selectCtx = ctx.selectClause();
            LogicalPlan selectPlan;
            LogicalPlan relation;
            if (ctx.fromClause() == null) {
                SelectColumnClauseContext columnCtx = selectCtx.selectColumnClause();
                if (columnCtx.EXCEPT() != null) {
                    throw new ParseException("select-except cannot be used in one row relation", selectCtx);
                }
                relation = new UnboundOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                        ImmutableList.of(new UnboundAlias(Literal.of(0))));
            } else {
                relation = visitFromClause(ctx.fromClause());
            }
            if (ctx.intoClause() != null && !ConnectContext.get().isRunProcedure()) {
                throw new ParseException("Only procedure supports insert into variables", selectCtx);
            }
            selectPlan = withSelectQuerySpecification(
                    ctx, relation,
                    selectCtx,
                    Optional.ofNullable(ctx.whereClause()),
                    Optional.ofNullable(ctx.aggClause()),
                    Optional.ofNullable(ctx.havingClause()));
            selectPlan = withQueryOrganization(selectPlan, ctx.queryOrganization());
            return withSelectHint(selectPlan, selectCtx.selectHint());
        });
    }

    @Override
    public LogicalPlan visitInlineTable(InlineTableContext ctx) {
        List<List<NamedExpression>> values = ctx.rowConstructor().stream()
                .map(this::visitRowConstructor)
                .collect(ImmutableList.toImmutableList());
        return new LogicalInlineTable(values);
    }

    /**
     * Create an aliased table reference. This is typically used in FROM clauses.
     */
    protected LogicalPlan withTableAlias(LogicalPlan plan, TableAliasContext ctx) {
        if (ctx.strictIdentifier() == null) {
            return plan;
        }
        return ParserUtils.withOrigin(ctx.strictIdentifier(), () -> {
            String alias = ctx.strictIdentifier().getText();
            if (null != ctx.identifierList()) {
                throw new ParseException("Do not implemented", ctx);
                // TODO: multi-colName
            }
            return new LogicalSubQueryAlias<>(alias, plan);
        });
    }

    @Override
    public LogicalPlan visitTableName(TableNameContext ctx) {
        List<String> tableId = visitMultipartIdentifier(ctx.multipartIdentifier());
        List<String> partitionNames = new ArrayList<>();
        boolean isTempPart = false;
        if (ctx.specifiedPartition() != null) {
            isTempPart = ctx.specifiedPartition().TEMPORARY() != null;
            if (ctx.specifiedPartition().identifier() != null) {
                partitionNames.add(ctx.specifiedPartition().identifier().getText());
            } else {
                partitionNames.addAll(visitIdentifierList(ctx.specifiedPartition().identifierList()));
            }
        }

        Optional<String> indexName = Optional.empty();
        if (ctx.materializedViewName() != null) {
            indexName = Optional.ofNullable(ctx.materializedViewName().indexName.getText());
        }

        List<Long> tabletIdLists = new ArrayList<>();
        if (ctx.tabletList() != null) {
            ctx.tabletList().tabletIdList.stream().forEach(tabletToken -> {
                tabletIdLists.add(Long.parseLong(tabletToken.getText()));
            });
        }

        final List<String> relationHints;
        if (ctx.relationHint() != null) {
            relationHints = typedVisit(ctx.relationHint());
        } else {
            relationHints = ImmutableList.of();
        }

        TableScanParams scanParams = null;
        if (ctx.optScanParams() != null) {
            Map<String, String> map = visitPropertyItemList(ctx.optScanParams().properties);
            scanParams = new TableScanParams(ctx.optScanParams().funcName.getText(), map);
        }

        TableSnapshot tableSnapshot = null;
        if (ctx.tableSnapshot() != null) {
            if (ctx.tableSnapshot().TIME() != null) {
                tableSnapshot = new TableSnapshot(stripQuotes(ctx.tableSnapshot().time.getText()));
            } else {
                tableSnapshot = new TableSnapshot(Long.parseLong(ctx.tableSnapshot().version.getText()));
            }
        }

        TableSample tableSample = ctx.sample() == null ? null : (TableSample) visit(ctx.sample());
        UnboundRelation relation = new UnboundRelation(StatementScopeIdGenerator.newRelationId(),
                        tableId, partitionNames, isTempPart, tabletIdLists, relationHints,
                        Optional.ofNullable(tableSample), indexName, scanParams, Optional.ofNullable(tableSnapshot));
        LogicalPlan checkedRelation = LogicalPlanBuilderAssistant.withCheckPolicy(relation);
        LogicalPlan plan = withTableAlias(checkedRelation, ctx.tableAlias());
        for (LateralViewContext lateralViewContext : ctx.lateralView()) {
            plan = withGenerate(plan, lateralViewContext);
        }
        return plan;
    }

    public static String stripQuotes(String str) {
        if ((str.charAt(0) == '\'' && str.charAt(str.length() - 1) == '\'')
                || (str.charAt(0) == '\"' && str.charAt(str.length() - 1) == '\"')) {
            str = str.substring(1, str.length() - 1);
        }
        return str;
    }

    @Override
    public LogicalPlan visitAliasedQuery(AliasedQueryContext ctx) {
        if (ctx.tableAlias().getText().equals("")) {
            throw new ParseException("Every derived table must have its own alias", ctx);
        }
        LogicalPlan plan = withTableAlias(visitQuery(ctx.query()), ctx.tableAlias());
        for (LateralViewContext lateralViewContext : ctx.lateralView()) {
            plan = withGenerate(plan, lateralViewContext);
        }
        return plan;
    }

    @Override
    public LogicalPlan visitTableValuedFunction(TableValuedFunctionContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String functionName = ctx.tvfName.getText();

            Map<String, String> map = visitPropertyItemList(ctx.properties);
            LogicalPlan relation = new UnboundTVFRelation(StatementScopeIdGenerator.newRelationId(),
                    functionName, new Properties(map));
            return withTableAlias(relation, ctx.tableAlias());
        });
    }

    /**
     * Create a star (i.e. all) expression; this selects all elements (in the specified object).
     * Both un-targeted (global) and targeted aliases are supported.
     */
    @Override
    public Expression visitStar(StarContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            final QualifiedNameContext qualifiedNameContext = ctx.qualifiedName();
            List<String> target;
            if (qualifiedNameContext != null) {
                target = qualifiedNameContext.identifier()
                        .stream()
                        .map(RuleContext::getText)
                        .collect(ImmutableList.toImmutableList());
            } else {
                target = ImmutableList.of();
            }
            return new UnboundStar(target);
        });
    }

    /**
     * Create an aliased expression if an alias is specified. Both single and multi-aliases are
     * supported.
     */
    @Override
    public NamedExpression visitNamedExpression(NamedExpressionContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression expression = getExpression(ctx.expression());
            if (ctx.identifierOrText() == null) {
                if (expression instanceof NamedExpression) {
                    return (NamedExpression) expression;
                } else {
                    return new UnboundAlias(expression);
                }
            }
            String alias = visitIdentifierOrText(ctx.identifierOrText());
            return new UnboundAlias(expression, alias);
        });
    }

    @Override
    public Expression visitSystemVariable(SystemVariableContext ctx) {
        VariableType type = null;
        if (ctx.kind == null) {
            type = VariableType.DEFAULT;
        } else if (ctx.kind.getType() == DorisParser.SESSION) {
            type = VariableType.SESSION;
        } else if (ctx.kind.getType() == DorisParser.GLOBAL) {
            type = VariableType.GLOBAL;
        }
        if (type == null) {
            throw new ParseException("Unsupported system variable: " + ctx.getText(), ctx);
        }
        return new UnboundVariable(ctx.identifier().getText(), type);
    }

    @Override
    public Expression visitUserVariable(UserVariableContext ctx) {
        return new UnboundVariable(ctx.identifierOrText().getText(), VariableType.USER);
    }

    /**
     * Create a comparison expression. This compares two expressions. The following comparison
     * operators are supported:
     * - Equal: '=' or '=='
     * - Null-safe Equal: '<=>'
     * - Not Equal: '<>' or '!='
     * - Less than: '<'
     * - Less then or Equal: '<='
     * - Greater than: '>'
     * - Greater then or Equal: '>='
     */
    @Override
    public Expression visitComparison(ComparisonContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);
            TerminalNode operator = (TerminalNode) ctx.comparisonOperator().getChild(0);
            switch (operator.getSymbol().getType()) {
                case DorisParser.EQ:
                    return new EqualTo(left, right);
                case DorisParser.NEQ:
                    return new Not(new EqualTo(left, right));
                case DorisParser.LT:
                    return new LessThan(left, right);
                case DorisParser.GT:
                    return new GreaterThan(left, right);
                case DorisParser.LTE:
                    return new LessThanEqual(left, right);
                case DorisParser.GTE:
                    return new GreaterThanEqual(left, right);
                case DorisParser.NSEQ:
                    return new NullSafeEqual(left, right);
                default:
                    throw new ParseException("Unsupported comparison expression: "
                        + operator.getSymbol().getText(), ctx);
            }
        });
    }

    /**
     * Create a not expression.
     * format: NOT Expression
     * for example:
     * not 1
     * not 1=1
     */
    @Override
    public Expression visitLogicalNot(LogicalNotContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> new Not(getExpression(ctx.booleanExpression())));
    }

    @Override
    public Expression visitLogicalBinary(LogicalBinaryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            // Code block copy from Spark
            // sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala

            // Collect all similar left hand contexts.
            List<BooleanExpressionContext> contexts = Lists.newArrayList(ctx.right);
            BooleanExpressionContext current = ctx.left;
            while (true) {
                if (current instanceof LogicalBinaryContext
                        && ((LogicalBinaryContext) current).operator.getType() == ctx.operator.getType()) {
                    contexts.add(((LogicalBinaryContext) current).right);
                    current = ((LogicalBinaryContext) current).left;
                } else {
                    contexts.add(current);
                    break;
                }
            }
            // Reverse the contexts to have them in the same sequence as in the SQL statement & turn them
            // into expressions.
            Collections.reverse(contexts);
            List<Expression> expressions = contexts.stream().map(this::getExpression).collect(Collectors.toList());
            // Create a balanced tree.
            return reduceToExpressionTree(0, expressions.size() - 1, expressions, ctx);
        });
    }

    @Override
    public Expression visitLambdaExpression(LambdaExpressionContext ctx) {
        ImmutableList<String> args = ctx.args.stream()
                .map(RuleContext::getText)
                .collect(ImmutableList.toImmutableList());
        Expression body = (Expression) visit(ctx.body);
        return new Lambda(args, body);
    }

    private Expression expressionCombiner(Expression left, Expression right, LogicalBinaryContext ctx) {
        switch (ctx.operator.getType()) {
            case DorisParser.LOGICALAND:
            case DorisParser.AND:
                return new And(left, right);
            case DorisParser.OR:
                return new Or(left, right);
            case DorisParser.XOR:
                return new Xor(left, right);
            default:
                throw new ParseException("Unsupported logical binary type: " + ctx.operator.getText(), ctx);
        }
    }

    private Expression reduceToExpressionTree(int low, int high,
            List<Expression> expressions, LogicalBinaryContext ctx) {
        switch (high - low) {
            case 0:
                return expressions.get(low);
            case 1:
                return expressionCombiner(expressions.get(low), expressions.get(high), ctx);
            default:
                int mid = low + (high - low) / 2;
                return expressionCombiner(
                        reduceToExpressionTree(low, mid, expressions, ctx),
                        reduceToExpressionTree(mid + 1, high, expressions, ctx),
                        ctx
                );
        }
    }

    /**
     * Create a predicated expression. A predicated expression is a normal expression with a
     * predicate attached to it, for example:
     * {{{
     * a + 1 IS NULL
     * }}}
     */
    @Override
    public Expression visitPredicated(PredicatedContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression e = getExpression(ctx.valueExpression());
            return ctx.predicate() == null ? e : withPredicate(e, ctx.predicate());
        });
    }

    @Override
    public Expression visitArithmeticUnary(ArithmeticUnaryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression e = typedVisit(ctx.valueExpression());
            switch (ctx.operator.getType()) {
                case DorisParser.PLUS:
                    return e;
                case DorisParser.SUBTRACT:
                    IntegerLiteral zero = new IntegerLiteral(0);
                    return new Subtract(zero, e);
                case DorisParser.TILDE:
                    return new BitNot(e);
                default:
                    throw new ParseException("Unsupported arithmetic unary type: " + ctx.operator.getText(), ctx);
            }
        });
    }

    @Override
    public Expression visitBitOperation(BitOperationContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);
            if (ctx.operator.getType() == DorisParser.BITAND) {
                return new BitAnd(left, right);
            } else if (ctx.operator.getType() == DorisParser.BITOR) {
                return new BitOr(left, right);
            } else if (ctx.operator.getType() == DorisParser.BITXOR) {
                return new BitXor(left, right);
            }
            throw new ParseException(" not supported", ctx);
        });
    }

    @Override
    public Expression visitArithmeticBinary(ArithmeticBinaryContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);

            int type = ctx.operator.getType();
            if (left instanceof Interval) {
                if (type != DorisParser.PLUS) {
                    throw new ParseException("Only supported: " + Operator.ADD, ctx);
                }
                Interval interval = (Interval) left;
                return new TimestampArithmetic(Operator.ADD, right, interval.value(), interval.timeUnit());
            }

            if (right instanceof Interval) {
                Operator op;
                if (type == DorisParser.PLUS) {
                    op = Operator.ADD;
                } else if (type == DorisParser.SUBTRACT) {
                    op = Operator.SUBTRACT;
                } else {
                    throw new ParseException("Only supported: " + Operator.ADD + " and " + Operator.SUBTRACT, ctx);
                }
                Interval interval = (Interval) right;
                return new TimestampArithmetic(op, left, interval.value(), interval.timeUnit());
            }

            return ParserUtils.withOrigin(ctx, () -> {
                switch (type) {
                    case DorisParser.ASTERISK:
                        return new Multiply(left, right);
                    case DorisParser.SLASH:
                        return new Divide(left, right);
                    case DorisParser.MOD:
                        return new Mod(left, right);
                    case DorisParser.PLUS:
                        return new Add(left, right);
                    case DorisParser.SUBTRACT:
                        return new Subtract(left, right);
                    case DorisParser.DIV:
                        return new IntegralDivide(left, right);
                    case DorisParser.HAT:
                        return new BitXor(left, right);
                    case DorisParser.PIPE:
                        return new BitOr(left, right);
                    case DorisParser.AMPERSAND:
                        return new BitAnd(left, right);
                    default:
                        throw new ParseException(
                                "Unsupported arithmetic binary type: " + ctx.operator.getText(), ctx);
                }
            });
        });
    }

    @Override
    public Expression visitTimestampdiff(TimestampdiffContext ctx) {
        Expression start = (Expression) visit(ctx.startTimestamp);
        Expression end = (Expression) visit(ctx.endTimestamp);
        String unit = ctx.unit.getText();
        if ("YEAR".equalsIgnoreCase(unit)) {
            return new YearsDiff(end, start);
        } else if ("MONTH".equalsIgnoreCase(unit)) {
            return new MonthsDiff(end, start);
        } else if ("WEEK".equalsIgnoreCase(unit)) {
            return new WeeksDiff(end, start);
        } else if ("DAY".equalsIgnoreCase(unit)) {
            return new DaysDiff(end, start);
        } else if ("HOUR".equalsIgnoreCase(unit)) {
            return new HoursDiff(end, start);
        } else if ("MINUTE".equalsIgnoreCase(unit)) {
            return new MinutesDiff(end, start);
        } else if ("SECOND".equalsIgnoreCase(unit)) {
            return new SecondsDiff(end, start);
        }
        throw new ParseException("Unsupported time stamp diff time unit: " + unit
                + ", supported time unit: YEAR/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND", ctx);

    }

    @Override
    public Expression visitTimestampadd(TimestampaddContext ctx) {
        Expression start = (Expression) visit(ctx.startTimestamp);
        Expression end = (Expression) visit(ctx.endTimestamp);
        String unit = ctx.unit.getText();
        if ("YEAR".equalsIgnoreCase(unit)) {
            return new YearsAdd(end, start);
        } else if ("MONTH".equalsIgnoreCase(unit)) {
            return new MonthsAdd(end, start);
        } else if ("WEEK".equalsIgnoreCase(unit)) {
            return new WeeksAdd(end, start);
        } else if ("DAY".equalsIgnoreCase(unit)) {
            return new DaysAdd(end, start);
        } else if ("HOUR".equalsIgnoreCase(unit)) {
            return new HoursAdd(end, start);
        } else if ("MINUTE".equalsIgnoreCase(unit)) {
            return new MinutesAdd(end, start);
        } else if ("SECOND".equalsIgnoreCase(unit)) {
            return new SecondsAdd(end, start);
        }
        throw new ParseException("Unsupported time stamp add time unit: " + unit
                + ", supported time unit: YEAR/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND", ctx);

    }

    @Override
    public Expression visitDate_add(Date_addContext ctx) {
        Expression timeStamp = (Expression) visit(ctx.timestamp);
        Expression amount = (Expression) visit(ctx.unitsAmount);
        if (ctx.unit == null) {
            //use "DAY" as unit by default
            return new DaysAdd(timeStamp, amount);
        }

        if ("Year".equalsIgnoreCase(ctx.unit.getText())) {
            return new YearsAdd(timeStamp, amount);
        } else if ("MONTH".equalsIgnoreCase(ctx.unit.getText())) {
            return new MonthsAdd(timeStamp, amount);
        } else if ("WEEK".equalsIgnoreCase(ctx.unit.getText())) {
            return new WeeksAdd(timeStamp, amount);
        } else if ("DAY".equalsIgnoreCase(ctx.unit.getText())) {
            return new DaysAdd(timeStamp, amount);
        } else if ("Hour".equalsIgnoreCase(ctx.unit.getText())) {
            return new HoursAdd(timeStamp, amount);
        } else if ("Minute".equalsIgnoreCase(ctx.unit.getText())) {
            return new MinutesAdd(timeStamp, amount);
        } else if ("Second".equalsIgnoreCase(ctx.unit.getText())) {
            return new SecondsAdd(timeStamp, amount);
        }
        throw new ParseException("Unsupported time unit: " + ctx.unit
                + ", supported time unit: YEAR/MONTH/DAY/HOUR/MINUTE/SECOND", ctx);
    }

    @Override
    public Expression visitArrayRange(ArrayRangeContext ctx) {
        Expression start = (Expression) visit(ctx.start);
        Expression end = (Expression) visit(ctx.end);
        Expression step = (Expression) visit(ctx.unitsAmount);

        String unit = ctx.unit == null ? null : ctx.unit.getText();
        if (unit != null && !unit.isEmpty()) {
            if ("Year".equalsIgnoreCase(unit)) {
                return new ArrayRangeYearUnit(start, end, step);
            } else if ("Month".equalsIgnoreCase(unit)) {
                return new ArrayRangeMonthUnit(start, end, step);
            } else if ("Week".equalsIgnoreCase(unit)) {
                return new ArrayRangeWeekUnit(start, end, step);
            } else if ("Day".equalsIgnoreCase(unit)) {
                return new ArrayRangeDayUnit(start, end, step);
            } else if ("Hour".equalsIgnoreCase(unit)) {
                return new ArrayRangeHourUnit(start, end, step);
            } else if ("Minute".equalsIgnoreCase(unit)) {
                return new ArrayRangeMinuteUnit(start, end, step);
            } else if ("Second".equalsIgnoreCase(unit)) {
                return new ArrayRangeSecondUnit(start, end, step);
            }
            throw new ParseException("Unsupported time unit: " + ctx.unit
                    + ", supported time unit: YEAR/MONTH/DAY/HOUR/MINUTE/SECOND", ctx);
        } else if (ctx.unitsAmount != null) {
            return new ArrayRange(start, end, step);
        } else if (ctx.end != null) {
            return new ArrayRange(start, end);
        } else {
            return new ArrayRange(start);
        }
    }

    @Override
    public Expression visitDate_sub(Date_subContext ctx) {
        Expression timeStamp = (Expression) visit(ctx.timestamp);
        Expression amount = (Expression) visit(ctx.unitsAmount);
        if (ctx.unit == null) {
            //use "DAY" as unit by default
            return new DaysSub(timeStamp, amount);
        }

        if ("Year".equalsIgnoreCase(ctx.unit.getText())) {
            return new YearsSub(timeStamp, amount);
        } else if ("MONTH".equalsIgnoreCase(ctx.unit.getText())) {
            return new MonthsSub(timeStamp, amount);
        } else if ("WEEK".equalsIgnoreCase(ctx.unit.getText())) {
            return new WeeksSub(timeStamp, amount);
        } else if ("DAY".equalsIgnoreCase(ctx.unit.getText())) {
            return new DaysSub(timeStamp, amount);
        } else if ("Hour".equalsIgnoreCase(ctx.unit.getText())) {
            return new HoursSub(timeStamp, amount);
        } else if ("Minute".equalsIgnoreCase(ctx.unit.getText())) {
            return new MinutesSub(timeStamp, amount);
        } else if ("Second".equalsIgnoreCase(ctx.unit.getText())) {
            return new SecondsSub(timeStamp, amount);
        }
        throw new ParseException("Unsupported time unit: " + ctx.unit
                + ", supported time unit: YEAR/MONTH/DAY/HOUR/MINUTE/SECOND", ctx);
    }

    @Override
    public Expression visitDateFloor(DateFloorContext ctx) {
        Expression timeStamp = (Expression) visit(ctx.timestamp);
        Expression amount = (Expression) visit(ctx.unitsAmount);
        if (ctx.unit == null) {
            // use "SECOND" as unit by default
            return new SecondFloor(timeStamp, amount);
        }
        Expression e = new DateTimeV2Literal(0001L, 01L, 01L, 0L, 0L, 0L, 0L);

        if ("Year".equalsIgnoreCase(ctx.unit.getText())) {
            return new YearFloor(timeStamp, amount, e);
        } else if ("MONTH".equalsIgnoreCase(ctx.unit.getText())) {
            return new MonthFloor(timeStamp, amount, e);
        } else if ("WEEK".equalsIgnoreCase(ctx.unit.getText())) {
            return new WeekFloor(timeStamp, amount, e);
        } else if ("DAY".equalsIgnoreCase(ctx.unit.getText())) {
            return new DayFloor(timeStamp, amount, e);
        } else if ("Hour".equalsIgnoreCase(ctx.unit.getText())) {
            return new HourFloor(timeStamp, amount, e);
        } else if ("Minute".equalsIgnoreCase(ctx.unit.getText())) {
            return new MinuteFloor(timeStamp, amount, e);
        } else if ("Second".equalsIgnoreCase(ctx.unit.getText())) {
            return new SecondFloor(timeStamp, amount, e);
        }
        throw new ParseException("Unsupported time unit: " + ctx.unit
                + ", supported time unit: YEAR/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND", ctx);
    }

    @Override
    public Expression visitDateCeil(DateCeilContext ctx) {
        Expression timeStamp = (Expression) visit(ctx.timestamp);
        Expression amount = (Expression) visit(ctx.unitsAmount);
        if (ctx.unit == null) {
            // use "Second" as unit by default
            return new SecondCeil(timeStamp, amount);
        }
        DateTimeV2Literal e = new DateTimeV2Literal(0001L, 01L, 01L, 0L, 0L, 0L, 0L);

        if ("Year".equalsIgnoreCase(ctx.unit.getText())) {
            return new YearCeil(timeStamp, amount, e);
        } else if ("MONTH".equalsIgnoreCase(ctx.unit.getText())) {
            return new MonthCeil(timeStamp, amount, e);
        } else if ("WEEK".equalsIgnoreCase(ctx.unit.getText())) {
            return new WeekCeil(timeStamp, amount, e);
        } else if ("DAY".equalsIgnoreCase(ctx.unit.getText())) {
            return new DayCeil(timeStamp, amount, e);
        } else if ("Hour".equalsIgnoreCase(ctx.unit.getText())) {
            return new HourCeil(timeStamp, amount, e);
        } else if ("Minute".equalsIgnoreCase(ctx.unit.getText())) {
            return new MinuteCeil(timeStamp, amount, e);
        } else if ("Second".equalsIgnoreCase(ctx.unit.getText())) {
            return new SecondCeil(timeStamp, amount, e);
        }
        throw new ParseException("Unsupported time unit: " + ctx.unit
                + ", supported time unit: YEAR/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND", ctx);
    }

    @Override
    public Expression visitCurrentDate(DorisParser.CurrentDateContext ctx) {
        return new CurrentDate().alias("CURRENT_DATE");
    }

    @Override
    public Expression visitCurrentTime(DorisParser.CurrentTimeContext ctx) {
        return new CurrentTime().alias("CURRENT_TIME");
    }

    @Override
    public Expression visitCurrentTimestamp(DorisParser.CurrentTimestampContext ctx) {
        return new Now().alias("CURRENT_TIMESTAMP");
    }

    @Override
    public Expression visitLocalTime(DorisParser.LocalTimeContext ctx) {
        return new CurrentTime().alias("LOCALTIME");
    }

    @Override
    public Expression visitLocalTimestamp(DorisParser.LocalTimestampContext ctx) {
        return new Now().alias("LOCALTIMESTAMP");
    }

    @Override
    public Expression visitCurrentUser(DorisParser.CurrentUserContext ctx) {
        return new CurrentUser().alias("CURRENT_USER");
    }

    @Override
    public Expression visitDoublePipes(DorisParser.DoublePipesContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression left = getExpression(ctx.left);
            Expression right = getExpression(ctx.right);
            if (SqlModeHelper.hasPipeAsConcat()) {
                return new UnboundFunction("concat", Lists.newArrayList(left, right));
            } else {
                return new Or(left, right);
            }
        });
    }

    /**
     * Create a value based [[CaseWhen]] expression. This has the following SQL form:
     * {{{
     *   CASE [expression]
     *    WHEN [value] THEN [expression]
     *    ...
     *    ELSE [expression]
     *   END
     * }}}
     */
    @Override
    public Expression visitSimpleCase(DorisParser.SimpleCaseContext context) {
        Expression e = getExpression(context.value);
        List<WhenClause> whenClauses = context.whenClause().stream()
                .map(w -> new WhenClause(new EqualTo(e, getExpression(w.condition)), getExpression(w.result)))
                .collect(ImmutableList.toImmutableList());
        if (context.elseExpression == null) {
            return new CaseWhen(whenClauses);
        }
        return new CaseWhen(whenClauses, getExpression(context.elseExpression));
    }

    /**
     * Create a condition based [[CaseWhen]] expression. This has the following SQL syntax:
     * {{{
     *   CASE
     *    WHEN [predicate] THEN [expression]
     *    ...
     *    ELSE [expression]
     *   END
     * }}}
     *
     * @param context the parse tree
     */
    @Override
    public Expression visitSearchedCase(DorisParser.SearchedCaseContext context) {
        List<WhenClause> whenClauses = context.whenClause().stream()
                .map(w -> new WhenClause(getExpression(w.condition), getExpression(w.result)))
                .collect(ImmutableList.toImmutableList());
        if (context.elseExpression == null) {
            return new CaseWhen(whenClauses);
        }
        return new CaseWhen(whenClauses, getExpression(context.elseExpression));
    }

    @Override
    public Expression visitCast(DorisParser.CastContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> processCast(getExpression(ctx.expression()), ctx.castDataType()));
    }

    @Override
    public UnboundFunction visitExtract(DorisParser.ExtractContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String functionName = ctx.field.getText();
            return new UnboundFunction(functionName, false,
                    Collections.singletonList(getExpression(ctx.source)));
        });
    }

    @Override
    public Expression visitEncryptKey(DorisParser.EncryptKeyContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String db = ctx.dbName == null ? "" : ctx.dbName.getText();
            String key = ctx.keyName.getText();
            return new EncryptKeyRef(new StringLiteral(db), new StringLiteral(key));
        });
    }

    @Override
    public Expression visitCharFunction(DorisParser.CharFunctionContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String charSet = ctx.charSet == null ? "utf8" : ctx.charSet.getText();
            List<Expression> arguments = ImmutableList.<Expression>builder()
                    .add(new StringLiteral(charSet))
                    .addAll(visit(ctx.arguments, Expression.class))
                    .build();
            return new Char(arguments);
        });
    }

    @Override
    public Expression visitConvertCharSet(DorisParser.ConvertCharSetContext ctx) {
        return ParserUtils.withOrigin(ctx,
                () -> new ConvertTo(getExpression(ctx.argument), new StringLiteral(ctx.charSet.getText())));
    }

    @Override
    public Expression visitConvertType(DorisParser.ConvertTypeContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> processCast(getExpression(ctx.argument), ctx.castDataType()));
    }

    @Override
    public DataType visitCastDataType(CastDataTypeContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            if (ctx.dataType() != null) {
                return ((DataType) typedVisit(ctx.dataType())).conversion();
            } else if (ctx.UNSIGNED() != null) {
                return LargeIntType.UNSIGNED;
            } else {
                return BigIntType.SIGNED;
            }
        });
    }

    private Expression processCast(Expression expression, CastDataTypeContext castDataTypeContext) {
        DataType dataType = visitCastDataType(castDataTypeContext);
        Expression cast = new Cast(expression, dataType, true);
        if (dataType.isStringLikeType() && ((CharacterType) dataType).getLen() >= 0) {
            if (dataType.isVarcharType() && ((VarcharType) dataType).isWildcardVarchar()) {
                return cast;
            }
            List<Expression> args = ImmutableList.of(
                    cast,
                    new TinyIntLiteral((byte) 1),
                    Literal.of(((CharacterType) dataType).getLen())
            );
            return new UnboundFunction("substr", args);
        } else {
            return cast;
        }
    }

    @Override
    public Expression visitFunctionCallExpression(DorisParser.FunctionCallExpressionContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String functionName = ctx.functionIdentifier().functionNameIdentifier().getText();
            boolean isDistinct = ctx.DISTINCT() != null;
            List<Expression> params = visit(ctx.expression(), Expression.class);
            List<OrderKey> orderKeys = visit(ctx.sortItem(), OrderKey.class);
            if (!orderKeys.isEmpty()) {
                return parseFunctionWithOrderKeys(functionName, isDistinct, params, orderKeys, ctx);
            }

            List<UnboundStar> unboundStars = ExpressionUtils.collectAll(params, UnboundStar.class::isInstance);
            if (!unboundStars.isEmpty()) {
                if (ctx.functionIdentifier().dbName == null && functionName.equalsIgnoreCase("count")) {
                    if (unboundStars.size() > 1) {
                        throw new ParseException(
                                "'*' can only be used once in conjunction with COUNT: " + functionName, ctx);
                    }
                    if (!unboundStars.get(0).getQualifier().isEmpty()) {
                        throw new ParseException("'*' can not has qualifier: " + unboundStars.size(), ctx);
                    }
                    if (ctx.windowSpec() != null) {
                        if (isDistinct) {
                            throw new ParseException("DISTINCT not allowed in analytic function: " + functionName, ctx);
                        }
                        return withWindowSpec(ctx.windowSpec(), new Count());
                    }
                    return new Count();
                }
                throw new ParseException("'*' can only be used in conjunction with COUNT: " + functionName, ctx);
            } else {
                String dbName = null;
                if (ctx.functionIdentifier().dbName != null) {
                    dbName = ctx.functionIdentifier().dbName.getText();
                }
                UnboundFunction function = new UnboundFunction(dbName, functionName, isDistinct, params);
                if (ctx.windowSpec() != null) {
                    if (isDistinct) {
                        throw new ParseException("DISTINCT not allowed in analytic function: " + functionName, ctx);
                    }
                    return withWindowSpec(ctx.windowSpec(), function);
                }
                return function;
            }
        });
    }

    /**
     * deal with window function definition
     */
    private WindowExpression withWindowSpec(WindowSpecContext ctx, Expression function) {
        List<Expression> partitionKeyList = Lists.newArrayList();
        if (ctx.partitionClause() != null) {
            partitionKeyList = visit(ctx.partitionClause().expression(), Expression.class);
        }

        List<OrderExpression> orderKeyList = Lists.newArrayList();
        if (ctx.sortClause() != null) {
            orderKeyList = visit(ctx.sortClause().sortItem(), OrderKey.class).stream()
                .map(orderKey -> new OrderExpression(orderKey))
                .collect(Collectors.toList());
        }

        if (ctx.windowFrame() != null) {
            return new WindowExpression(function, partitionKeyList, orderKeyList, withWindowFrame(ctx.windowFrame()));
        }
        return new WindowExpression(function, partitionKeyList, orderKeyList);
    }

    /**
     * deal with optional expressions
     */
    private <T, C> Optional<C> optionalVisit(T ctx, Supplier<C> func) {
        return Optional.ofNullable(ctx).map(a -> func.get());
    }

    /**
     * deal with window frame
     */
    private WindowFrame withWindowFrame(WindowFrameContext ctx) {
        WindowFrame.FrameUnitsType frameUnitsType = WindowFrame.FrameUnitsType.valueOf(
                ctx.frameUnits().getText().toUpperCase());
        WindowFrame.FrameBoundary leftBoundary = withFrameBound(ctx.start);
        if (ctx.end != null) {
            WindowFrame.FrameBoundary rightBoundary = withFrameBound(ctx.end);
            return new WindowFrame(frameUnitsType, leftBoundary, rightBoundary);
        }
        return new WindowFrame(frameUnitsType, leftBoundary);
    }

    private WindowFrame.FrameBoundary withFrameBound(DorisParser.FrameBoundaryContext ctx) {
        Optional<Expression> expression = Optional.empty();
        if (ctx.expression() != null) {
            expression = Optional.of(getExpression(ctx.expression()));
            // todo: use isConstant() to resolve Function in expression; currently we only
            //  support literal expression
            if (!expression.get().isLiteral()) {
                throw new ParseException("Unsupported expression in WindowFrame : " + expression, ctx);
            }
        }

        WindowFrame.FrameBoundType frameBoundType = null;
        switch (ctx.boundType.getType()) {
            case DorisParser.PRECEDING:
                if (ctx.UNBOUNDED() != null) {
                    frameBoundType = WindowFrame.FrameBoundType.UNBOUNDED_PRECEDING;
                } else {
                    frameBoundType = WindowFrame.FrameBoundType.PRECEDING;
                }
                break;
            case DorisParser.CURRENT:
                frameBoundType = WindowFrame.FrameBoundType.CURRENT_ROW;
                break;
            case DorisParser.FOLLOWING:
                if (ctx.UNBOUNDED() != null) {
                    frameBoundType = WindowFrame.FrameBoundType.UNBOUNDED_FOLLOWING;
                } else {
                    frameBoundType = WindowFrame.FrameBoundType.FOLLOWING;
                }
                break;
            default:
        }
        return new WindowFrame.FrameBoundary(expression, frameBoundType);
    }

    @Override
    public Expression visitInterval(IntervalContext ctx) {
        return new Interval(getExpression(ctx.value), visitUnitIdentifier(ctx.unit));
    }

    @Override
    public String visitUnitIdentifier(UnitIdentifierContext ctx) {
        return ctx.getText();
    }

    @Override
    public Literal visitTypeConstructor(TypeConstructorContext ctx) {
        String value = ctx.STRING_LITERAL().getText();
        value = value.substring(1, value.length() - 1);
        String type = ctx.type.getText().toUpperCase();
        switch (type) {
            case "DATE":
                return Config.enable_date_conversion ? new DateV2Literal(value) : new DateLiteral(value);
            case "TIMESTAMP":
                return Config.enable_date_conversion ? new DateTimeV2Literal(value) : new DateTimeLiteral(value);
            case "DATEV2":
                return new DateV2Literal(value);
            case "DATEV1":
                return new DateLiteral(value);
            default:
                throw new ParseException("Unsupported data type : " + type, ctx);
        }
    }

    @Override
    public Expression visitDereference(DereferenceContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression e = getExpression(ctx.base);
            if (e instanceof UnboundSlot) {
                UnboundSlot unboundAttribute = (UnboundSlot) e;
                List<String> nameParts = Lists.newArrayList(unboundAttribute.getNameParts());
                nameParts.add(ctx.fieldName.getText());
                UnboundSlot slot = new UnboundSlot(nameParts, Optional.empty());
                return slot;
            } else {
                // todo: base is an expression, may be not a table name.
                throw new ParseException("Unsupported dereference expression: " + ctx.getText(), ctx);
            }
        });
    }

    @Override
    public Expression visitElementAt(ElementAtContext ctx) {
        return new ElementAt(typedVisit(ctx.value), typedVisit(ctx.index));
    }

    @Override
    public Expression visitArraySlice(ArraySliceContext ctx) {
        if (ctx.end != null) {
            return new ArraySlice(typedVisit(ctx.value), typedVisit(ctx.begin), typedVisit(ctx.end));
        } else {
            return new ArraySlice(typedVisit(ctx.value), typedVisit(ctx.begin));
        }
    }

    @Override
    public Expression visitColumnReference(ColumnReferenceContext ctx) {
        // todo: handle quoted and unquoted
        return UnboundSlot.quoted(ctx.getText());
    }

    /**
     * Create a NULL literal expression.
     */
    @Override
    public Literal visitNullLiteral(NullLiteralContext ctx) {
        return new NullLiteral();
    }

    @Override
    public Literal visitBooleanLiteral(BooleanLiteralContext ctx) {
        Boolean b = Boolean.valueOf(ctx.getText());
        return BooleanLiteral.of(b);
    }

    @Override
    public Literal visitIntegerLiteral(IntegerLiteralContext ctx) {
        BigInteger bigInt = new BigInteger(ctx.getText());
        if (BigInteger.valueOf(bigInt.byteValue()).equals(bigInt)) {
            return new TinyIntLiteral(bigInt.byteValue());
        } else if (BigInteger.valueOf(bigInt.shortValue()).equals(bigInt)) {
            return new SmallIntLiteral(bigInt.shortValue());
        } else if (BigInteger.valueOf(bigInt.intValue()).equals(bigInt)) {
            return new IntegerLiteral(bigInt.intValue());
        } else if (BigInteger.valueOf(bigInt.longValue()).equals(bigInt)) {
            return new BigIntLiteral(bigInt.longValueExact());
        } else {
            return new LargeIntLiteral(bigInt);
        }
    }

    @Override
    public Literal visitStringLiteral(StringLiteralContext ctx) {
        String txt = ctx.STRING_LITERAL().getText();
        String s = txt.substring(1, txt.length() - 1);
        if (txt.charAt(0) == '\'') {
            // for single quote string, '' should be converted to '
            s = s.replace("''", "'");
        } else if (txt.charAt(0) == '"') {
            // for double quote string, "" should be converted to "
            s = s.replace("\"\"", "\"");
        }
        if (!SqlModeHelper.hasNoBackSlashEscapes()) {
            s = LogicalPlanBuilderAssistant.escapeBackSlash(s);
        }
        int strLength = Utils.containChinese(s) ? s.length() * StringLikeLiteral.CHINESE_CHAR_BYTE_LENGTH : s.length();
        if (strLength > ScalarType.MAX_VARCHAR_LENGTH) {
            return new StringLiteral(s);
        }
        return new VarcharLiteral(s, strLength);
    }

    @Override
    public Expression visitPlaceholder(DorisParser.PlaceholderContext ctx) {
        Placeholder parameter = new Placeholder(ConnectContext.get().getStatementContext().getNextPlaceholderId());
        tokenPosToParameters.put(ctx.start, parameter);
        return parameter;
    }

    /**
     * cast all items to same types.
     * TODO remove this function after we refactor type coercion.
     */
    private List<Literal> typeCoercionItems(List<Literal> items) {
        Array array = new Array(items.toArray(new Literal[0]));
        if (array.expectedInputTypes().isEmpty()) {
            return ImmutableList.of();
        }
        DataType dataType = array.expectedInputTypes().get(0);
        return items.stream()
                .map(item -> item.checkedCastTo(dataType))
                .map(Literal.class::cast)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public ArrayLiteral visitArrayLiteral(ArrayLiteralContext ctx) {
        List<Literal> items = ctx.items.stream().<Literal>map(this::typedVisit).collect(Collectors.toList());
        if (items.isEmpty()) {
            return new ArrayLiteral(items);
        }
        return new ArrayLiteral(typeCoercionItems(items));
    }

    @Override
    public MapLiteral visitMapLiteral(MapLiteralContext ctx) {
        List<Literal> items = ctx.items.stream().<Literal>map(this::typedVisit).collect(Collectors.toList());
        if (items.size() % 2 != 0) {
            throw new ParseException("map can't be odd parameters, need even parameters", ctx);
        }
        List<Literal> keys = Lists.newArrayList();
        List<Literal> values = Lists.newArrayList();
        for (int i = 0; i < items.size(); i++) {
            if (i % 2 == 0) {
                keys.add(items.get(i));
            } else {
                values.add(items.get(i));
            }
        }
        return new MapLiteral(typeCoercionItems(keys), typeCoercionItems(values));
    }

    @Override
    public Object visitStructLiteral(StructLiteralContext ctx) {
        List<Literal> fields = ctx.items.stream().<Literal>map(this::typedVisit).collect(Collectors.toList());
        return new StructLiteral(fields);
    }

    @Override
    public Expression visitParenthesizedExpression(ParenthesizedExpressionContext ctx) {
        return getExpression(ctx.expression());
    }

    @Override
    public List<NamedExpression> visitRowConstructor(RowConstructorContext ctx) {
        return ctx.rowConstructorItem().stream()
                .map(this::visitRowConstructorItem)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public NamedExpression visitRowConstructorItem(RowConstructorItemContext ctx) {
        if (ctx.DEFAULT() != null) {
            return new DefaultValueSlot();
        } else {
            return visitNamedExpression(ctx.namedExpression());
        }
    }

    @Override
    public List<Expression> visitNamedExpressionSeq(NamedExpressionSeqContext namedCtx) {
        return visit(namedCtx.namedExpression(), Expression.class);
    }

    @Override
    public LogicalPlan visitRelation(RelationContext ctx) {
        return plan(ctx.relationPrimary());
    }

    @Override
    public LogicalPlan visitFromClause(FromClauseContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> visitRelations(ctx.relations()));
    }

    @Override
    public LogicalPlan visitRelations(DorisParser.RelationsContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> withRelations(null, ctx.relation()));
    }

    @Override
    public LogicalPlan visitRelationList(DorisParser.RelationListContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> withRelations(null, ctx.relations().relation()));
    }

    /* ********************************************************************************************
     * Table Identifier parsing
     * ******************************************************************************************** */

    @Override
    public List<String> visitMultipartIdentifier(MultipartIdentifierContext ctx) {
        return ctx.parts.stream()
            .map(RuleContext::getText)
            .collect(ImmutableList.toImmutableList());
    }

    /**
     * Create a Sequence of Strings for a parenthesis enclosed alias list.
     */
    @Override
    public List<String> visitIdentifierList(IdentifierListContext ctx) {
        return visitIdentifierSeq(ctx.identifierSeq());
    }

    /**
     * Create a Sequence of Strings for an identifier list.
     */
    @Override
    public List<String> visitIdentifierSeq(IdentifierSeqContext ctx) {
        return ctx.ident.stream()
            .map(RuleContext::getText)
            .collect(ImmutableList.toImmutableList());
    }

    @Override
    public EqualTo visitUpdateAssignment(UpdateAssignmentContext ctx) {
        return new EqualTo(new UnboundSlot(visitMultipartIdentifier(ctx.multipartIdentifier()), Optional.empty()),
                getExpression(ctx.expression()));
    }

    @Override
    public List<EqualTo> visitUpdateAssignmentSeq(UpdateAssignmentSeqContext ctx) {
        return ctx.assignments.stream()
                .map(this::visitUpdateAssignment)
                .collect(Collectors.toList());
    }

    /**
     * get OrderKey.
     *
     * @param ctx SortItemContext
     * @return SortItems
     */
    @Override
    public OrderKey visitSortItem(SortItemContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            boolean isAsc = ctx.DESC() == null;
            boolean isNullFirst = ctx.FIRST() != null || (ctx.LAST() == null && isAsc);
            Expression expression = typedVisit(ctx.expression());
            return new OrderKey(expression, isAsc, isNullFirst);
        });
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(ImmutableList.toImmutableList());
    }

    private LogicalPlan plan(ParserRuleContext tree) {
        return (LogicalPlan) tree.accept(this);
    }

    /* ********************************************************************************************
     * create table parsing
     * ******************************************************************************************** */

    @Override
    public LogicalPlan visitCreateView(CreateViewContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        String comment = ctx.STRING_LITERAL() == null ? "" : LogicalPlanBuilderAssistant.escapeBackSlash(
                ctx.STRING_LITERAL().getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1));
        String querySql = getOriginSql(ctx.query());
        CreateViewInfo info = new CreateViewInfo(ctx.EXISTS() != null, new TableNameInfo(nameParts),
                comment, querySql,
                ctx.cols == null ? Lists.newArrayList() : visitSimpleColumnDefs(ctx.cols));
        return new CreateViewCommand(info);
    }

    @Override
    public LogicalPlan visitCreateTable(CreateTableContext ctx) {
        String ctlName = null;
        String dbName = null;
        String tableName = null;
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        // TODO: support catalog
        if (nameParts.size() == 1) {
            tableName = nameParts.get(0);
        } else if (nameParts.size() == 2) {
            dbName = nameParts.get(0);
            tableName = nameParts.get(1);
        } else if (nameParts.size() == 3) {
            ctlName = nameParts.get(0);
            dbName = nameParts.get(1);
            tableName = nameParts.get(2);
        } else {
            throw new AnalysisException("nameParts in create table should be [ctl.][db.]tbl");
        }
        KeysType keysType = null;
        if (ctx.DUPLICATE() != null) {
            keysType = KeysType.DUP_KEYS;
        } else if (ctx.AGGREGATE() != null) {
            keysType = KeysType.AGG_KEYS;
        } else if (ctx.UNIQUE() != null) {
            keysType = KeysType.UNIQUE_KEYS;
        }
        // when engineName is null, get engineName from current catalog later
        String engineName = ctx.engine != null ? ctx.engine.getText().toLowerCase() : null;
        int bucketNum = FeConstants.default_bucket_num;
        if (ctx.INTEGER_VALUE() != null) {
            bucketNum = Integer.parseInt(ctx.INTEGER_VALUE().getText());
        }
        String comment = ctx.STRING_LITERAL() == null ? "" : LogicalPlanBuilderAssistant.escapeBackSlash(
                ctx.STRING_LITERAL().getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1));
        DistributionDescriptor desc = null;
        if (ctx.HASH() != null) {
            desc = new DistributionDescriptor(true, ctx.autoBucket != null, bucketNum,
                    visitIdentifierList(ctx.hashKeys));
        } else if (ctx.RANDOM() != null) {
            desc = new DistributionDescriptor(false, ctx.autoBucket != null, bucketNum, null);
        }
        Map<String, String> properties = ctx.properties != null
                // NOTICE: we should not generate immutable map here, because it will be modified when analyzing.
                ? Maps.newHashMap(visitPropertyClause(ctx.properties))
                : Maps.newHashMap();
        Map<String, String> extProperties = ctx.extProperties != null
                // NOTICE: we should not generate immutable map here, because it will be modified when analyzing.
                ? Maps.newHashMap(visitPropertyClause(ctx.extProperties))
                : Maps.newHashMap();

        // solve partition by
        PartitionTableInfo partitionInfo;
        if (ctx.partition != null) {
            partitionInfo = (PartitionTableInfo) ctx.partitionTable().accept(this);
        } else {
            partitionInfo = PartitionTableInfo.EMPTY;
        }

        if (ctx.columnDefs() != null) {
            if (ctx.AS() != null) {
                throw new AnalysisException("Should not define the entire column in CTAS");
            }
            return new CreateTableCommand(Optional.empty(), new CreateTableInfo(
                    ctx.EXISTS() != null,
                    ctx.EXTERNAL() != null,
                    ctlName,
                    dbName,
                    tableName,
                    visitColumnDefs(ctx.columnDefs()),
                    ctx.indexDefs() != null ? visitIndexDefs(ctx.indexDefs()) : ImmutableList.of(),
                    engineName,
                    keysType,
                    ctx.keys != null ? visitIdentifierList(ctx.keys) : ImmutableList.of(),
                    comment,
                    partitionInfo,
                    desc,
                    ctx.rollupDefs() != null ? visitRollupDefs(ctx.rollupDefs()) : ImmutableList.of(),
                    properties,
                    extProperties,
                    ctx.clusterKeys != null ? visitIdentifierList(ctx.clusterKeys) : ImmutableList.of()));
        } else if (ctx.AS() != null) {
            return new CreateTableCommand(Optional.of(visitQuery(ctx.query())), new CreateTableInfo(
                    ctx.EXISTS() != null,
                    ctx.EXTERNAL() != null,
                    ctlName,
                    dbName,
                    tableName,
                    ctx.ctasCols != null ? visitIdentifierList(ctx.ctasCols) : null,
                    engineName,
                    keysType,
                    ctx.keys != null ? visitIdentifierList(ctx.keys) : ImmutableList.of(),
                    comment,
                    partitionInfo,
                    desc,
                    ctx.rollupDefs() != null ? visitRollupDefs(ctx.rollupDefs()) : ImmutableList.of(),
                    properties,
                    extProperties,
                    ctx.clusterKeys != null ? visitIdentifierList(ctx.clusterKeys) : ImmutableList.of()));
        } else {
            throw new AnalysisException("Should contain at least one column in a table");
        }
    }

    @Override
    public PartitionTableInfo visitPartitionTable(DorisParser.PartitionTableContext ctx) {
        boolean isAutoPartition = ctx.autoPartition != null;
        ImmutableList<Expression> partitionList = ctx.partitionList.identityOrFunction().stream()
                .map(partition -> {
                    IdentifierContext identifier = partition.identifier();
                    if (identifier != null) {
                        return UnboundSlot.quoted(identifier.getText());
                    } else {
                        return visitFunctionCallExpression(partition.functionCallExpression());
                    }
                })
                .collect(ImmutableList.toImmutableList());
        return new PartitionTableInfo(
            isAutoPartition,
            ctx.RANGE() != null ? "RANGE" : "LIST",
            ctx.partitions != null ? visitPartitionsDef(ctx.partitions) : null,
            partitionList);
    }

    @Override
    public List<ColumnDefinition> visitColumnDefs(ColumnDefsContext ctx) {
        return ctx.cols.stream().map(this::visitColumnDef).collect(Collectors.toList());
    }

    @Override
    public ColumnDefinition visitColumnDef(ColumnDefContext ctx) {
        String colName = ctx.colName.getText();
        DataType colType = ctx.type instanceof PrimitiveDataTypeContext
                ? visitPrimitiveDataType(((PrimitiveDataTypeContext) ctx.type))
                : ctx.type instanceof ComplexDataTypeContext
                        ? visitComplexDataType((ComplexDataTypeContext) ctx.type)
                        : visitAggStateDataType((AggStateDataTypeContext) ctx.type);
        colType = colType.conversion();
        boolean isKey = ctx.KEY() != null;
        ColumnNullableType nullableType = ColumnNullableType.DEFAULT;
        if (ctx.NOT() != null) {
            nullableType = ColumnNullableType.NOT_NULLABLE;
        } else if (ctx.nullable != null) {
            nullableType = ColumnNullableType.NULLABLE;
        }
        String aggTypeString = ctx.aggType != null ? ctx.aggType.getText() : null;
        Optional<DefaultValue> defaultValue = Optional.empty();
        Optional<DefaultValue> onUpdateDefaultValue = Optional.empty();
        if (ctx.DEFAULT() != null) {
            if (ctx.INTEGER_VALUE() != null) {
                defaultValue = Optional.of(new DefaultValue(ctx.INTEGER_VALUE().getText()));
            } else if (ctx.DECIMAL_VALUE() != null) {
                defaultValue = Optional.of(new DefaultValue(ctx.DECIMAL_VALUE().getText()));
            } else if (ctx.stringValue != null) {
                defaultValue = Optional.of(new DefaultValue(toStringValue(ctx.stringValue.getText())));
            } else if (ctx.nullValue != null) {
                defaultValue = Optional.of(DefaultValue.NULL_DEFAULT_VALUE);
            } else if (ctx.defaultTimestamp != null) {
                if (ctx.defaultValuePrecision == null) {
                    defaultValue = Optional.of(DefaultValue.CURRENT_TIMESTAMP_DEFAULT_VALUE);
                } else {
                    defaultValue = Optional.of(DefaultValue
                            .currentTimeStampDefaultValueWithPrecision(
                                    Long.valueOf(ctx.defaultValuePrecision.getText())));
                }
            } else if (ctx.CURRENT_DATE() != null) {
                defaultValue = Optional.of(DefaultValue.CURRENT_DATE_DEFAULT_VALUE);
            } else if (ctx.PI() != null) {
                defaultValue = Optional.of(DefaultValue.PI_DEFAULT_VALUE);
            }
        }
        if (ctx.UPDATE() != null) {
            if (ctx.onUpdateValuePrecision == null) {
                onUpdateDefaultValue = Optional.of(DefaultValue.CURRENT_TIMESTAMP_DEFAULT_VALUE);
            } else {
                onUpdateDefaultValue = Optional.of(DefaultValue
                        .currentTimeStampDefaultValueWithPrecision(
                                Long.valueOf(ctx.onUpdateValuePrecision.getText())));
            }
        }
        AggregateType aggType = null;
        if (aggTypeString != null) {
            try {
                aggType = AggregateType.valueOf(aggTypeString.toUpperCase());
            } catch (Exception e) {
                throw new AnalysisException(String.format("Aggregate type %s is unsupported", aggTypeString),
                        e.getCause());
            }
        }
        //comment should remove '\' and '(") at the beginning and end
        String comment = ctx.comment != null ? ctx.comment.getText().substring(1, ctx.comment.getText().length() - 1)
                .replace("\\", "") : "";
        long autoIncInitValue = -1;
        if (ctx.AUTO_INCREMENT() != null) {
            if (ctx.autoIncInitValue != null) {
                // AUTO_INCREMENT(Value) Value >= 0.
                autoIncInitValue = Long.valueOf(ctx.autoIncInitValue.getText());
                if (autoIncInitValue < 0) {
                    throw new AnalysisException("AUTO_INCREMENT start value can not be negative.");
                }
            } else {
                // AUTO_INCREMENT default 1.
                autoIncInitValue = Long.valueOf(1);
            }
        }
        Optional<GeneratedColumnDesc> desc = ctx.generatedExpr != null
                ? Optional.of(new GeneratedColumnDesc(ctx.generatedExpr.getText(), getExpression(ctx.generatedExpr)))
                : Optional.empty();
        return new ColumnDefinition(colName, colType, isKey, aggType, nullableType, autoIncInitValue, defaultValue,
                onUpdateDefaultValue, comment, desc);
    }

    @Override
    public List<IndexDefinition> visitIndexDefs(IndexDefsContext ctx) {
        return ctx.indexes.stream().map(this::visitIndexDef).collect(Collectors.toList());
    }

    @Override
    public IndexDefinition visitIndexDef(IndexDefContext ctx) {
        String indexName = ctx.indexName.getText();
        List<String> indexCols = visitIdentifierList(ctx.cols);
        Map<String, String> properties = visitPropertyItemList(ctx.properties);
        String indexType = ctx.indexType != null ? ctx.indexType.getText().toUpperCase() : null;
        //comment should remove '\' and '(") at the beginning and end
        String comment = ctx.comment == null ? "" : LogicalPlanBuilderAssistant.escapeBackSlash(
                        ctx.comment.getText().substring(1, ctx.STRING_LITERAL().getText().length() - 1));
        // change BITMAP index to INVERTED index
        if (Config.enable_create_bitmap_index_as_inverted_index
                && "BITMAP".equalsIgnoreCase(indexType)) {
            indexType = "INVERTED";
        }
        return new IndexDefinition(indexName, indexCols, indexType, properties, comment);
    }

    @Override
    public List<PartitionDefinition> visitPartitionsDef(PartitionsDefContext ctx) {
        return ctx.partitions.stream()
                .map(p -> ((PartitionDefinition) visit(p))).collect(Collectors.toList());
    }

    @Override
    public PartitionDefinition visitPartitionDef(DorisParser.PartitionDefContext ctx) {
        PartitionDefinition partitionDefinition = (PartitionDefinition) visit(ctx.getChild(0));
        if (ctx.partitionProperties != null) {
            partitionDefinition.withProperties(visitPropertyItemList(ctx.partitionProperties));
        }
        return partitionDefinition;
    }

    @Override
    public PartitionDefinition visitLessThanPartitionDef(LessThanPartitionDefContext ctx) {
        String partitionName = ctx.partitionName.getText();
        if (ctx.MAXVALUE() == null) {
            List<Expression> lessThanValues = visitConstantSeq(ctx.constantSeq());
            return new LessThanPartition(ctx.EXISTS() != null, partitionName, lessThanValues);
        } else {
            return new LessThanPartition(ctx.EXISTS() != null, partitionName,
                    ImmutableList.of(MaxValue.INSTANCE));
        }
    }

    @Override
    public PartitionDefinition visitFixedPartitionDef(FixedPartitionDefContext ctx) {
        String partitionName = ctx.partitionName.getText();
        List<Expression> lowerBounds = visitConstantSeq(ctx.lower);
        List<Expression> upperBounds = visitConstantSeq(ctx.upper);
        return new FixedRangePartition(ctx.EXISTS() != null, partitionName, lowerBounds, upperBounds);
    }

    @Override
    public PartitionDefinition visitStepPartitionDef(StepPartitionDefContext ctx) {
        List<Expression> fromExpression = visitConstantSeq(ctx.from);
        List<Expression> toExpression = visitConstantSeq(ctx.to);
        return new StepPartition(false, null, fromExpression, toExpression,
                Long.parseLong(ctx.unitsAmount.getText()), ctx.unit != null ? ctx.unit.getText() : null);
    }

    @Override
    public PartitionDefinition visitInPartitionDef(InPartitionDefContext ctx) {
        List<List<Expression>> values;
        if (ctx.constants == null) {
            values = ctx.constantSeqs.stream().map(this::visitConstantSeq)
                    .collect(Collectors.toList());
        } else {
            values = visitConstantSeq(ctx.constants).stream().map(ImmutableList::of)
                    .collect(Collectors.toList());
        }
        return new InPartition(ctx.EXISTS() != null, ctx.partitionName.getText(), values);
    }

    @Override
    public List<Expression> visitConstantSeq(ConstantSeqContext ctx) {
        return ctx.values.stream()
                .map(this::visitPartitionValueDef)
                .collect(Collectors.toList());
    }

    @Override
    public Expression visitPartitionValueDef(PartitionValueDefContext ctx) {
        if (ctx.INTEGER_VALUE() != null) {
            return Literal.of(ctx.INTEGER_VALUE().getText());
        } else if (ctx.STRING_LITERAL() != null) {
            return Literal.of(toStringValue(ctx.STRING_LITERAL().getText()));
        } else if (ctx.MAXVALUE() != null) {
            return MaxValue.INSTANCE;
        } else if (ctx.NULL() != null) {
            return Literal.of(null);
        }
        throw new AnalysisException("Unsupported partition value: " + ctx.getText());
    }

    @Override
    public List<RollupDefinition> visitRollupDefs(RollupDefsContext ctx) {
        return ctx.rollups.stream().map(this::visitRollupDef).collect(Collectors.toList());
    }

    @Override
    public RollupDefinition visitRollupDef(RollupDefContext ctx) {
        String rollupName = ctx.rollupName.getText();
        List<String> rollupCols = visitIdentifierList(ctx.rollupCols);
        List<String> dupKeys = ctx.dupKeys == null ? ImmutableList.of() : visitIdentifierList(ctx.dupKeys);
        Map<String, String> properties = ctx.properties == null ? Maps.newHashMap()
                : visitPropertyClause(ctx.properties);
        return new RollupDefinition(rollupName, rollupCols, dupKeys, properties);
    }

    private String toStringValue(String literal) {
        return literal.substring(1, literal.length() - 1);
    }

    /* ********************************************************************************************
     * Expression parsing
     * ******************************************************************************************** */

    /**
     * Create an expression from the given context. This method just passes the context on to the
     * visitor and only takes care of typing (We assume that the visitor returns an Expression here).
     */
    private Expression getExpression(ParserRuleContext ctx) {
        return typedVisit(ctx);
    }

    private LogicalPlan withExplain(LogicalPlan inputPlan, ExplainContext ctx) {
        if (ctx == null) {
            return inputPlan;
        }
        return ParserUtils.withOrigin(ctx, () -> {
            ExplainLevel explainLevel = ExplainLevel.NORMAL;

            if (ctx.planType() != null) {
                if (ctx.level == null || !ctx.level.getText().equalsIgnoreCase("plan")) {
                    throw new ParseException("Only explain plan can use plan type: " + ctx.planType().getText(), ctx);
                }
            }

            boolean showPlanProcess = false;
            if (ctx.level != null) {
                if (!ctx.level.getText().equalsIgnoreCase("plan")) {
                    explainLevel = ExplainLevel.valueOf(ctx.level.getText().toUpperCase(Locale.ROOT));
                } else {
                    explainLevel = parseExplainPlanType(ctx.planType());

                    if (ctx.PROCESS() != null) {
                        showPlanProcess = true;
                    }
                }
            }
            return new ExplainCommand(explainLevel, inputPlan, showPlanProcess);
        });
    }

    private LogicalPlan withOutFile(LogicalPlan plan, OutFileClauseContext ctx) {
        if (ctx == null) {
            return plan;
        }
        String format = "csv";
        if (ctx.format != null) {
            format = ctx.format.getText();
        }

        Map<String, String> properties = ImmutableMap.of();
        if (ctx.propertyClause() != null) {
            properties = visitPropertyClause(ctx.propertyClause());
        }
        Literal filePath = (Literal) visit(ctx.filePath);
        return new LogicalFileSink<>(filePath.getStringValue(), format, properties, ImmutableList.of(), plan);
    }

    private LogicalPlan withQueryOrganization(LogicalPlan inputPlan, QueryOrganizationContext ctx) {
        if (ctx == null) {
            return inputPlan;
        }
        Optional<SortClauseContext> sortClauseContext = Optional.ofNullable(ctx.sortClause());
        Optional<LimitClauseContext> limitClauseContext = Optional.ofNullable(ctx.limitClause());
        LogicalPlan sort = withSort(inputPlan, sortClauseContext);
        return withLimit(sort, limitClauseContext);
    }

    private LogicalPlan withSort(LogicalPlan input, Optional<SortClauseContext> sortCtx) {
        return input.optionalMap(sortCtx, () -> {
            List<OrderKey> orderKeys = visit(sortCtx.get().sortItem(), OrderKey.class);
            return new LogicalSort<>(orderKeys, input);
        });
    }

    private LogicalPlan withLimit(LogicalPlan input, Optional<LimitClauseContext> limitCtx) {
        return input.optionalMap(limitCtx, () -> {
            long limit = Long.parseLong(limitCtx.get().limit.getText());
            if (limit < 0) {
                throw new ParseException("Limit requires non-negative number", limitCtx.get());
            }
            long offset = 0;
            Token offsetToken = limitCtx.get().offset;
            if (offsetToken != null) {
                offset = Long.parseLong(offsetToken.getText());
            }
            return new LogicalLimit<>(limit, offset, LimitPhase.ORIGIN, input);
        });
    }

    private UnboundOneRowRelation withOneRowRelation(SelectColumnClauseContext selectCtx) {
        return ParserUtils.withOrigin(selectCtx, () -> {
            // fromClause does not exists.
            List<NamedExpression> projects = getNamedExpressions(selectCtx.namedExpressionSeq());
            return new UnboundOneRowRelation(StatementScopeIdGenerator.newRelationId(), projects);
        });
    }

    /**
     * Add a regular (SELECT) query specification to a logical plan. The query specification
     * is the core of the logical plan, this is where sourcing (FROM clause), projection (SELECT),
     * aggregation (GROUP BY ... HAVING ...) and filtering (WHERE) takes place.
     *
     * <p>Note that query hints are ignored (both by the parser and the builder).
     */
    protected LogicalPlan withSelectQuerySpecification(
            ParserRuleContext ctx,
            LogicalPlan inputRelation,
            SelectClauseContext selectClause,
            Optional<WhereClauseContext> whereClause,
            Optional<AggClauseContext> aggClause,
            Optional<HavingClauseContext> havingClause) {
        return ParserUtils.withOrigin(ctx, () -> {
            // from -> where -> group by -> having -> select
            LogicalPlan filter = withFilter(inputRelation, whereClause);
            SelectColumnClauseContext selectColumnCtx = selectClause.selectColumnClause();
            LogicalPlan aggregate = withAggregate(filter, selectColumnCtx, aggClause);
            boolean isDistinct = (selectClause.DISTINCT() != null);
            if (!(aggregate instanceof Aggregate) && havingClause.isPresent()) {
                // create a project node for pattern match of ProjectToGlobalAggregate rule
                // then ProjectToGlobalAggregate rule can insert agg node as LogicalHaving node's child
                LogicalPlan project;
                if (selectColumnCtx.EXCEPT() != null) {
                    List<NamedExpression> expressions = getNamedExpressions(selectColumnCtx.namedExpressionSeq());
                    if (!expressions.stream().allMatch(UnboundSlot.class::isInstance)) {
                        throw new ParseException("only column name is supported in except clause", selectColumnCtx);
                    }
                    UnboundStar star = new UnboundStar(ImmutableList.of());
                    project = new LogicalProject<>(ImmutableList.of(star), expressions, isDistinct, aggregate);
                } else {
                    List<NamedExpression> projects = getNamedExpressions(selectColumnCtx.namedExpressionSeq());
                    project = new LogicalProject<>(projects, ImmutableList.of(), isDistinct, aggregate);
                }
                return new LogicalHaving<>(ExpressionUtils.extractConjunctionToSet(
                        getExpression((havingClause.get().booleanExpression()))), project);
            } else {
                LogicalPlan having = withHaving(aggregate, havingClause);
                return withProjection(having, selectColumnCtx, aggClause, isDistinct);
            }
        });
    }

    /**
     * Join one more [[LogicalPlan]]s to the current logical plan.
     */
    private LogicalPlan withJoinRelations(LogicalPlan input, RelationContext ctx) {
        LogicalPlan last = input;
        for (JoinRelationContext join : ctx.joinRelation()) {
            JoinType joinType;
            if (join.joinType().CROSS() != null) {
                joinType = JoinType.CROSS_JOIN;
            } else if (join.joinType().FULL() != null) {
                joinType = JoinType.FULL_OUTER_JOIN;
            } else if (join.joinType().SEMI() != null) {
                if (join.joinType().LEFT() != null) {
                    joinType = JoinType.LEFT_SEMI_JOIN;
                } else {
                    joinType = JoinType.RIGHT_SEMI_JOIN;
                }
            } else if (join.joinType().ANTI() != null) {
                if (join.joinType().LEFT() != null) {
                    joinType = JoinType.LEFT_ANTI_JOIN;
                } else {
                    joinType = JoinType.RIGHT_ANTI_JOIN;
                }
            } else if (join.joinType().LEFT() != null) {
                joinType = JoinType.LEFT_OUTER_JOIN;
            } else if (join.joinType().RIGHT() != null) {
                joinType = JoinType.RIGHT_OUTER_JOIN;
            } else if (join.joinType().INNER() != null) {
                joinType = JoinType.INNER_JOIN;
            } else if (join.joinCriteria() != null) {
                joinType = JoinType.INNER_JOIN;
            } else {
                joinType = JoinType.CROSS_JOIN;
            }
            DistributeType distributeType = Optional.ofNullable(join.distributeType()).map(hintCtx -> {
                String hint = typedVisit(join.distributeType());
                if (DistributeType.JoinDistributeType.SHUFFLE.toString().equalsIgnoreCase(hint)) {
                    return DistributeType.SHUFFLE_RIGHT;
                } else if (DistributeType.JoinDistributeType.BROADCAST.toString().equalsIgnoreCase(hint)) {
                    return DistributeType.BROADCAST_RIGHT;
                } else {
                    throw new ParseException("Invalid join hint: " + hint, hintCtx);
                }
            }).orElse(DistributeType.NONE);
            DistributeHint distributeHint = new DistributeHint(distributeType);
            // TODO: natural join, lateral join, union join
            JoinCriteriaContext joinCriteria = join.joinCriteria();
            Optional<Expression> condition = Optional.empty();
            List<Expression> ids = null;
            if (joinCriteria != null) {
                if (join.joinType().CROSS() != null) {
                    throw new ParseException("Cross join can't be used with ON clause", joinCriteria);
                }
                if (joinCriteria.booleanExpression() != null) {
                    condition = Optional.ofNullable(getExpression(joinCriteria.booleanExpression()));
                } else if (joinCriteria.USING() != null) {
                    ids = visitIdentifierList(joinCriteria.identifierList())
                            .stream().map(UnboundSlot::quoted)
                            .collect(ImmutableList.toImmutableList());
                }
            } else {
                // keep same with original planner, allow cross/inner join
                if (!joinType.isInnerOrCrossJoin()) {
                    throw new ParseException("on mustn't be empty except for cross/inner join", join);
                }
            }
            if (ids == null) {
                last = new LogicalJoin<>(joinType, ExpressionUtils.EMPTY_CONDITION,
                        condition.map(ExpressionUtils::extractConjunction)
                                .orElse(ExpressionUtils.EMPTY_CONDITION),
                        distributeHint,
                        Optional.empty(),
                        last,
                        plan(join.relationPrimary()), null);
            } else {
                last = new UsingJoin<>(joinType, last,
                        plan(join.relationPrimary()), ImmutableList.of(), ids, distributeHint);

            }
            if (distributeHint.distributeType != DistributeType.NONE
                    && ConnectContext.get().getStatementContext() != null
                    && !ConnectContext.get().getStatementContext().getHints().contains(distributeHint)) {
                ConnectContext.get().getStatementContext().addHint(distributeHint);
            }
        }
        return last;
    }

    private LogicalPlan withSelectHint(LogicalPlan logicalPlan, SelectHintContext hintContext) {
        if (hintContext == null) {
            return logicalPlan;
        }
        Map<String, SelectHint> hints = Maps.newLinkedHashMap();
        for (HintStatementContext hintStatement : hintContext.hintStatements) {
            String hintName = hintStatement.hintName.getText().toLowerCase(Locale.ROOT);
            switch (hintName) {
                case "set_var":
                    Map<String, Optional<String>> parameters = Maps.newLinkedHashMap();
                    for (HintAssignmentContext kv : hintStatement.parameters) {
                        if (kv.key != null) {
                            String parameterName = visitIdentifierOrText(kv.key);
                            Optional<String> value = Optional.empty();
                            if (kv.constantValue != null) {
                                Literal literal = (Literal) visit(kv.constantValue);
                                value = Optional.ofNullable(literal.toLegacyLiteral().getStringValue());
                            } else if (kv.identifierValue != null) {
                                // maybe we should throw exception when the identifierValue is quoted identifier
                                value = Optional.ofNullable(kv.identifierValue.getText());
                            }
                            parameters.put(parameterName, value);
                        }
                    }
                    hints.put(hintName, new SelectHintSetVar(hintName, parameters));
                    break;
                case "leading":
                    List<String> leadingParameters = new ArrayList<String>();
                    for (HintAssignmentContext kv : hintStatement.parameters) {
                        if (kv.key != null) {
                            String parameterName = visitIdentifierOrText(kv.key);
                            leadingParameters.add(parameterName);
                        }
                    }
                    hints.put(hintName, new SelectHintLeading(hintName, leadingParameters));
                    break;
                case "ordered":
                    hints.put(hintName, new SelectHintOrdered(hintName));
                    break;
                case "use_cbo_rule":
                    List<String> useRuleParameters = new ArrayList<String>();
                    for (HintAssignmentContext kv : hintStatement.parameters) {
                        if (kv.key != null) {
                            String parameterName = visitIdentifierOrText(kv.key);
                            useRuleParameters.add(parameterName);
                        }
                    }
                    hints.put(hintName, new SelectHintUseCboRule(hintName, useRuleParameters, false));
                    break;
                case "no_use_cbo_rule":
                    List<String> noUseRuleParameters = new ArrayList<String>();
                    for (HintAssignmentContext kv : hintStatement.parameters) {
                        String parameterName = visitIdentifierOrText(kv.key);
                        if (kv.key != null) {
                            noUseRuleParameters.add(parameterName);
                        }
                    }
                    hints.put(hintName, new SelectHintUseCboRule(hintName, noUseRuleParameters, true));
                    break;
                default:
                    break;
            }
        }
        return new LogicalSelectHint<>(hints, logicalPlan);
    }

    @Override
    public String visitBracketDistributeType(BracketDistributeTypeContext ctx) {
        return ctx.identifier().getText();
    }

    @Override
    public String visitCommentDistributeType(CommentDistributeTypeContext ctx) {
        return ctx.identifier().getText();
    }

    @Override
    public List<String> visitBracketRelationHint(BracketRelationHintContext ctx) {
        return ctx.identifier().stream()
                .map(RuleContext::getText)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public Object visitCommentRelationHint(CommentRelationHintContext ctx) {
        return ctx.identifier().stream()
                .map(RuleContext::getText)
                .collect(ImmutableList.toImmutableList());
    }

    protected LogicalPlan withProjection(LogicalPlan input, SelectColumnClauseContext selectCtx,
            Optional<AggClauseContext> aggCtx, boolean isDistinct) {
        return ParserUtils.withOrigin(selectCtx, () -> {
            if (aggCtx.isPresent()) {
                if (isDistinct) {
                    return new LogicalProject<>(ImmutableList.of(new UnboundStar(ImmutableList.of())),
                            Collections.emptyList(), isDistinct, input);
                } else {
                    return input;
                }
            } else {
                if (selectCtx.EXCEPT() != null) {
                    List<NamedExpression> expressions = getNamedExpressions(selectCtx.namedExpressionSeq());
                    if (!expressions.stream().allMatch(UnboundSlot.class::isInstance)) {
                        throw new ParseException("only column name is supported in except clause", selectCtx);
                    }
                    UnboundStar star = new UnboundStar(ImmutableList.of());
                    return new LogicalProject<>(ImmutableList.of(star), expressions, isDistinct, input);
                } else {
                    List<NamedExpression> projects = getNamedExpressions(selectCtx.namedExpressionSeq());
                    return new LogicalProject<>(projects, Collections.emptyList(), isDistinct, input);
                }
            }
        });
    }

    private LogicalPlan withRelations(LogicalPlan inputPlan, List<RelationContext> relations) {
        if (relations == null) {
            return inputPlan;
        }
        LogicalPlan left = inputPlan;
        for (RelationContext relation : relations) {
            // build left deep join tree
            LogicalPlan right = withJoinRelations(visitRelation(relation), relation);
            left = (left == null) ? right :
                    new LogicalJoin<>(
                            JoinType.CROSS_JOIN,
                            ExpressionUtils.EMPTY_CONDITION,
                            ExpressionUtils.EMPTY_CONDITION,
                            new DistributeHint(DistributeType.NONE),
                            Optional.empty(),
                            left,
                            right, null);
            // TODO: pivot and lateral view
        }
        return left;
    }

    private LogicalPlan withFilter(LogicalPlan input, Optional<WhereClauseContext> whereCtx) {
        return input.optionalMap(whereCtx, () ->
            new LogicalFilter<>(ExpressionUtils.extractConjunctionToSet(
                    getExpression(whereCtx.get().booleanExpression())), input));
    }

    private LogicalPlan withAggregate(LogicalPlan input, SelectColumnClauseContext selectCtx,
                                      Optional<AggClauseContext> aggCtx) {
        return input.optionalMap(aggCtx, () -> {
            GroupingElementContext groupingElementContext = aggCtx.get().groupingElement();
            List<NamedExpression> namedExpressions = getNamedExpressions(selectCtx.namedExpressionSeq());
            if (groupingElementContext.GROUPING() != null) {
                ImmutableList.Builder<List<Expression>> groupingSets = ImmutableList.builder();
                for (GroupingSetContext groupingSetContext : groupingElementContext.groupingSet()) {
                    groupingSets.add(visit(groupingSetContext.expression(), Expression.class));
                }
                return new LogicalRepeat<>(groupingSets.build(), namedExpressions, input);
            } else if (groupingElementContext.CUBE() != null) {
                List<Expression> cubeExpressions = visit(groupingElementContext.expression(), Expression.class);
                List<List<Expression>> groupingSets = ExpressionUtils.cubeToGroupingSets(cubeExpressions);
                return new LogicalRepeat<>(groupingSets, namedExpressions, input);
            } else if (groupingElementContext.ROLLUP() != null) {
                List<Expression> rollupExpressions = visit(groupingElementContext.expression(), Expression.class);
                List<List<Expression>> groupingSets = ExpressionUtils.rollupToGroupingSets(rollupExpressions);
                return new LogicalRepeat<>(groupingSets, namedExpressions, input);
            } else {
                List<Expression> groupByExpressions = visit(groupingElementContext.expression(), Expression.class);
                return new LogicalAggregate<>(groupByExpressions, namedExpressions, input);
            }
        });
    }

    private LogicalPlan withHaving(LogicalPlan input, Optional<HavingClauseContext> havingCtx) {
        return input.optionalMap(havingCtx, () -> {
            if (!(input instanceof Aggregate)) {
                throw new ParseException("Having clause should be applied against an aggregation.", havingCtx.get());
            }
            return new LogicalHaving<>(ExpressionUtils.extractConjunctionToSet(
                    getExpression((havingCtx.get().booleanExpression()))), input);
        });
    }

    /**
     * match predicate type and generate different predicates.
     *
     * @param ctx PredicateContext
     * @param valueExpression valueExpression
     * @return Expression
     */
    private Expression withPredicate(Expression valueExpression, PredicateContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            Expression outExpression;
            switch (ctx.kind.getType()) {
                case DorisParser.BETWEEN:
                    outExpression = new And(
                            new GreaterThanEqual(valueExpression, getExpression(ctx.lower)),
                            new LessThanEqual(valueExpression, getExpression(ctx.upper))
                    );
                    break;
                case DorisParser.LIKE:
                    outExpression = new Like(
                        valueExpression,
                        getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.RLIKE:
                case DorisParser.REGEXP:
                    outExpression = new Regexp(
                        valueExpression,
                        getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.IN:
                    if (ctx.query() == null) {
                        outExpression = new InPredicate(
                                valueExpression,
                                withInList(ctx)
                        );
                    } else {
                        outExpression = new InSubquery(
                                valueExpression,
                                new ListQuery(typedVisit(ctx.query())),
                                ctx.NOT() != null
                        );
                    }
                    break;
                case DorisParser.NULL:
                    outExpression = new IsNull(valueExpression);
                    break;
                case DorisParser.MATCH:
                case DorisParser.MATCH_ANY:
                    outExpression = new MatchAny(
                        valueExpression,
                        getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.MATCH_ALL:
                    outExpression = new MatchAll(
                        valueExpression,
                        getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.MATCH_PHRASE:
                    outExpression = new MatchPhrase(
                        valueExpression,
                        getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.MATCH_PHRASE_PREFIX:
                    outExpression = new MatchPhrasePrefix(
                        valueExpression,
                        getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.MATCH_REGEXP:
                    outExpression = new MatchRegexp(
                        valueExpression,
                        getExpression(ctx.pattern)
                    );
                    break;
                case DorisParser.MATCH_PHRASE_EDGE:
                    outExpression = new MatchPhraseEdge(
                        valueExpression,
                        getExpression(ctx.pattern)
                    );
                    break;
                default:
                    throw new ParseException("Unsupported predicate type: " + ctx.kind.getText(), ctx);
            }
            return ctx.NOT() != null ? new Not(outExpression) : outExpression;
        });
    }

    private List<NamedExpression> getNamedExpressions(NamedExpressionSeqContext namedCtx) {
        return ParserUtils.withOrigin(namedCtx, () -> visit(namedCtx.namedExpression(), NamedExpression.class));
    }

    @Override
    public Expression visitSubqueryExpression(SubqueryExpressionContext subqueryExprCtx) {
        return ParserUtils.withOrigin(subqueryExprCtx, () -> new ScalarSubquery(typedVisit(subqueryExprCtx.query())));
    }

    @Override
    public Expression visitExist(ExistContext context) {
        return ParserUtils.withOrigin(context, () -> new Exists(typedVisit(context.query()), false));
    }

    @Override
    public Expression visitIsnull(IsnullContext context) {
        return ParserUtils.withOrigin(context, () -> new IsNull(typedVisit(context.valueExpression())));
    }

    @Override
    public Expression visitIs_not_null_pred(Is_not_null_predContext context) {
        return ParserUtils.withOrigin(context, () -> new Not(new IsNull(typedVisit(context.valueExpression()))));
    }

    public List<Expression> withInList(PredicateContext ctx) {
        return ctx.expression().stream().map(this::getExpression).collect(ImmutableList.toImmutableList());
    }

    @Override
    public Literal visitDecimalLiteral(DecimalLiteralContext ctx) {
        try {
            if (Config.enable_decimal_conversion) {
                return new DecimalV3Literal(new BigDecimal(ctx.getText()));
            } else {
                return new DecimalLiteral(new BigDecimal(ctx.getText()));
            }
        } catch (Exception e) {
            return new DoubleLiteral(Double.parseDouble(ctx.getText()));
        }
    }

    private String parsePropertyKey(PropertyKeyContext item) {
        if (item.constant() != null) {
            return parseConstant(item.constant());
        }
        return item.getText();
    }

    private String parsePropertyValue(PropertyValueContext item) {
        if (item.constant() != null) {
            return parseConstant(item.constant());
        }
        return item.getText();
    }

    private ExplainLevel parseExplainPlanType(PlanTypeContext planTypeContext) {
        if (planTypeContext == null || planTypeContext.ALL() != null) {
            return ExplainLevel.ALL_PLAN;
        }
        if (planTypeContext.PHYSICAL() != null || planTypeContext.OPTIMIZED() != null) {
            return ExplainLevel.OPTIMIZED_PLAN;
        }
        if (planTypeContext.REWRITTEN() != null || planTypeContext.LOGICAL() != null) {
            return ExplainLevel.REWRITTEN_PLAN;
        }
        if (planTypeContext.ANALYZED() != null) {
            return ExplainLevel.ANALYZED_PLAN;
        }
        if (planTypeContext.PARSED() != null) {
            return ExplainLevel.PARSED_PLAN;
        }
        if (planTypeContext.SHAPE() != null) {
            return ExplainLevel.SHAPE_PLAN;
        }
        if (planTypeContext.MEMO() != null) {
            return ExplainLevel.MEMO_PLAN;
        }
        if (planTypeContext.DISTRIBUTED() != null) {
            return ExplainLevel.DISTRIBUTED_PLAN;
        }
        return ExplainLevel.ALL_PLAN;
    }

    @Override
    public Pair<DataType, Boolean> visitDataTypeWithNullable(DataTypeWithNullableContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> Pair.of(typedVisit(ctx.dataType()), ctx.NOT() == null));
    }

    @Override
    public DataType visitAggStateDataType(AggStateDataTypeContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            List<Pair<DataType, Boolean>> dataTypeWithNullables = ctx.dataTypes.stream()
                    .map(this::visitDataTypeWithNullable)
                    .collect(Collectors.toList());
            List<DataType> dataTypes = dataTypeWithNullables.stream()
                    .map(dt -> dt.first)
                    .collect(ImmutableList.toImmutableList());
            List<Boolean> nullables = dataTypeWithNullables.stream()
                    .map(dt -> dt.second)
                    .collect(ImmutableList.toImmutableList());
            String functionName = ctx.functionNameIdentifier().getText();
            if (!BuiltinAggregateFunctions.INSTANCE.aggFuncNames.contains(functionName)) {
                // TODO use function binder to check function exists
                throw new ParseException("Can not found function '" + functionName + "'", ctx);
            }
            return new AggStateType(functionName, dataTypes, nullables);
        });
    }

    @Override
    public DataType visitPrimitiveDataType(PrimitiveDataTypeContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            String dataType = ctx.primitiveColType().type.getText().toLowerCase(Locale.ROOT);
            if (dataType.equalsIgnoreCase("all")) {
                throw new NotSupportedException("Disable to create table with `ALL` type columns");
            }
            List<String> l = Lists.newArrayList(dataType);
            ctx.INTEGER_VALUE().stream().map(ParseTree::getText).forEach(l::add);
            return DataType.convertPrimitiveFromStrings(l);
        });
    }

    @Override
    public DataType visitComplexDataType(ComplexDataTypeContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            switch (ctx.complex.getType()) {
                case DorisParser.ARRAY:
                    return ArrayType.of(typedVisit(ctx.dataType(0)), true);
                case DorisParser.MAP:
                    return MapType.of(typedVisit(ctx.dataType(0)), typedVisit(ctx.dataType(1)));
                case DorisParser.STRUCT:
                    return new StructType(visitComplexColTypeList(ctx.complexColTypeList()));
                default:
                    throw new AnalysisException("do not support " + ctx.complex.getText() + " type for Nereids");
            }
        });
    }

    @Override
    public List<StructField> visitComplexColTypeList(ComplexColTypeListContext ctx) {
        return ctx.complexColType().stream().map(this::visitComplexColType).collect(ImmutableList.toImmutableList());
    }

    @Override
    public StructField visitComplexColType(ComplexColTypeContext ctx) {
        String comment;
        if (ctx.commentSpec() != null) {
            comment = ctx.commentSpec().STRING_LITERAL().getText();
            comment = LogicalPlanBuilderAssistant.escapeBackSlash(comment.substring(1, comment.length() - 1));
        } else {
            comment = "";
        }
        return new StructField(ctx.identifier().getText(), typedVisit(ctx.dataType()), true, comment);
    }

    private Expression parseFunctionWithOrderKeys(String functionName, boolean isDistinct,
            List<Expression> params, List<OrderKey> orderKeys, ParserRuleContext ctx) {
        if (functionName.equalsIgnoreCase("group_concat")) {
            OrderExpression[] orderExpressions = orderKeys.stream()
                    .map(OrderExpression::new)
                    .toArray(OrderExpression[]::new);
            if (params.size() == 1) {
                return new GroupConcat(isDistinct, params.get(0), orderExpressions);
            } else if (params.size() == 2) {
                return new GroupConcat(isDistinct, params.get(0), params.get(1), orderExpressions);
            } else {
                throw new ParseException("group_concat requires one or two parameters: " + params, ctx);
            }
        }
        throw new ParseException("Unsupported function with order expressions" + ctx.getText(), ctx);
    }

    private String parseConstant(ConstantContext context) {
        Object constant = visit(context);
        if (constant instanceof Literal && ((Literal) constant).isStringLikeLiteral()) {
            return ((Literal) constant).getStringValue();
        }
        return context.getText();
    }

    @Override
    public Object visitCollate(CollateContext ctx) {
        return visit(ctx.primaryExpression());
    }

    @Override
    public Object visitSample(SampleContext ctx) {
        long seek = ctx.seed == null ? -1L : Long.parseLong(ctx.seed.getText());
        DorisParser.SampleMethodContext sampleContext = ctx.sampleMethod();
        if (sampleContext instanceof SampleByPercentileContext) {
            SampleByPercentileContext sampleByPercentileContext = (SampleByPercentileContext) sampleContext;
            long percent = Long.parseLong(sampleByPercentileContext.INTEGER_VALUE().getText());
            return new TableSample(percent, true, seek);
        }
        SampleByRowsContext sampleByRowsContext = (SampleByRowsContext) sampleContext;
        long rows = Long.parseLong(sampleByRowsContext.INTEGER_VALUE().getText());
        return new TableSample(rows, false, seek);
    }

    @Override
    public Object visitCallProcedure(CallProcedureContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        FuncNameInfo procedureName = new FuncNameInfo(nameParts);
        List<Expression> arguments = ctx.expression().stream()
                .<Expression>map(this::typedVisit)
                .collect(ImmutableList.toImmutableList());
        UnboundFunction unboundFunction = new UnboundFunction(procedureName.getDbName(), procedureName.getName(),
                true, arguments);
        return new CallCommand(unboundFunction, getOriginSql(ctx));
    }

    @Override
    public LogicalPlan visitCreateProcedure(CreateProcedureContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        FuncNameInfo procedureName = new FuncNameInfo(nameParts);
        return ParserUtils.withOrigin(ctx, () -> {
            LogicalPlan createProcedurePlan;
            createProcedurePlan = new CreateProcedureCommand(procedureName, getOriginSql(ctx),
                    ctx.REPLACE() != null);
            return createProcedurePlan;
        });
    }

    @Override
    public LogicalPlan visitDropProcedure(DropProcedureContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        FuncNameInfo procedureName = new FuncNameInfo(nameParts);
        return ParserUtils.withOrigin(ctx, () -> new DropProcedureCommand(procedureName, getOriginSql(ctx)));
    }

    @Override
    public LogicalPlan visitShowProcedureStatus(ShowProcedureStatusContext ctx) {
        Set<Expression> whereExpr = Collections.emptySet();
        if (ctx.whereClause() != null) {
            whereExpr = ExpressionUtils.extractConjunctionToSet(
                    getExpression(ctx.whereClause().booleanExpression()));
        }

        if (ctx.valueExpression() != null) {
            // parser allows only LIKE or WhereClause.
            // Mysql grammar: SHOW PROCEDURE STATUS [LIKE 'pattern' | WHERE expr]
            whereExpr = Sets.newHashSet(new Like(new UnboundSlot("ProcedureName"), getExpression(ctx.pattern)));
        }

        final Set<Expression> whereExprConst = whereExpr;
        return ParserUtils.withOrigin(ctx, () -> new ShowProcedureStatusCommand(whereExprConst));
    }

    @Override
    public LogicalPlan visitShowCreateProcedure(ShowCreateProcedureContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        FuncNameInfo procedureName = new FuncNameInfo(nameParts);
        return ParserUtils.withOrigin(ctx, () -> new ShowCreateProcedureCommand(procedureName));
    }

    @Override
    public LogicalPlan visitDropCatalogRecycleBin(DropCatalogRecycleBinContext ctx) {
        String idTypeStr = ctx.idType.getText().substring(1, ctx.idType.getText().length() - 1);
        IdType idType = IdType.fromString(idTypeStr);
        long id = Long.parseLong(ctx.id.getText());

        return ParserUtils.withOrigin(ctx, () -> new DropCatalogRecycleBinCommand(idType, id));
    }

    @Override
    public Object visitUnsupported(UnsupportedContext ctx) {
        return UnsupportedCommand.INSTANCE;
    }

    @Override
    public LogicalPlan visitCreateTableLike(CreateTableLikeContext ctx) {
        List<String> nameParts = visitMultipartIdentifier(ctx.name);
        List<String> existedTableNameParts = visitMultipartIdentifier(ctx.existedTable);
        ArrayList<String> rollupNames = Lists.newArrayList();
        boolean withAllRollUp = false;
        if (ctx.WITH() != null && ctx.rollupNames != null) {
            rollupNames = new ArrayList<>(visitIdentifierList(ctx.rollupNames));
        } else if (ctx.WITH() != null && ctx.rollupNames == null) {
            withAllRollUp = true;
        }
        CreateTableLikeInfo info = new CreateTableLikeInfo(ctx.EXISTS() != null,
                new TableNameInfo(nameParts), new TableNameInfo(existedTableNameParts),
                rollupNames, withAllRollUp);
        return new CreateTableLikeCommand(info);
    }
}
