/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.function;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.*;

import com.google.common.collect.Lists;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.cli.tsql.InvalidStatementException;
import org.apache.tajo.cli.tsql.ParsedResult;
import org.apache.tajo.cli.tsql.SimpleParser;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.CharDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.datum.TimeDatum;
import org.apache.tajo.datum.TimestampDatum;
import org.apache.tajo.engine.codegen.EvalCodeGenerator;
import org.apache.tajo.engine.codegen.TajoClassLoader;
import org.apache.tajo.engine.function.FunctionLoader;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamOptionTypes;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.function.Function;
import org.apache.tajo.function.FunctionSignature;
import org.apache.tajo.master.exec.QueryExecutor;
import org.apache.tajo.parser.sql.SQLAnalyzer;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.EvalContext;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.serder.EvalNodeDeserializer;
import org.apache.tajo.plan.serder.EvalNodeSerializer;
import org.apache.tajo.plan.serder.PlanProto.EvalNodeTree;
import org.apache.tajo.plan.verifier.LogicalPlanVerifier;
import org.apache.tajo.plan.verifier.PreLogicalPlanVerifier;
import org.apache.tajo.plan.verifier.VerificationState;
import org.apache.tajo.storage.LazyTuple;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.BytesUtils;
import org.apache.tajo.util.ClassUtil;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

public class ExprTest {
  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static CatalogService cat;
  private static SQLAnalyzer analyzer;
  private static PreLogicalPlanVerifier preLogicalPlanVerifier;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static LogicalPlanVerifier annotatedPlanVerifier;

  public static String getUserTimeZoneDisplay(TimeZone tz) {
    return DateTimeUtil.getTimeZoneDisplayTime(tz);
  }

  public ExprTest() {
  }

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    conf = util.getConfiguration();
    util.startCatalogCluster();
    cat = util.getCatalogService();
    cat.createTablespace("default", "hdfs://localhost:1234/warehouse");
    cat.createDatabase("default", "default");
    Map map = FunctionLoader.load();
    map = FunctionLoader.loadUserDefinedFunctions(conf, map);
    Iterator var1 = map.values().iterator();

    while(var1.hasNext()) {
      FunctionDesc funcDesc = (FunctionDesc)var1.next();
      cat.createFunction(funcDesc);
    }

    for (FunctionDesc funcDesc : findUDFs()) {
      cat.createFunction(funcDesc);
    }

    analyzer = new SQLAnalyzer();
    preLogicalPlanVerifier = new PreLogicalPlanVerifier(cat);
    planner = new LogicalPlanner(cat, TablespaceManager.getInstance());
    optimizer = new LogicalOptimizer(util.getConfiguration(), cat, TablespaceManager.getInstance());
    annotatedPlanVerifier = new LogicalPlanVerifier();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  /**
   * This method finds and build FunctionDesc for the legacy function and UD(A)F system.
   *
   * @return A list of FunctionDescs
   */
  public static List<FunctionDesc> findUDFs() {
    List<FunctionDesc> sqlFuncs = Lists.newArrayList();

    Set<Class> functionClasses = ClassUtil.findClasses(Function.class, "com.blrunner.tajo.udf");

    for (Class eachClass : functionClasses) {
      if(eachClass.isInterface() || Modifier.isAbstract(eachClass.getModifiers())) {
        continue;
      }
      Function function = null;
      try {
        function = (Function)eachClass.newInstance();
      } catch (Exception e) {
//        LOG.warn(eachClass + " cannot instantiate Function class because of " + e.getMessage(), e);
        continue;
      }
      String functionName = function.getClass().getAnnotation(Description.class).functionName();
      String[] synonyms = function.getClass().getAnnotation(Description.class).synonyms();
      String description = function.getClass().getAnnotation(Description.class).description();
      String detail = function.getClass().getAnnotation(Description.class).detail();
      String example = function.getClass().getAnnotation(Description.class).example();
      TajoDataTypes.Type returnType = function.getClass().getAnnotation(Description.class).returnType();
      ParamTypes[] paramArray = function.getClass().getAnnotation(Description.class).paramTypes();

      String[] allFunctionNames = null;
      if(synonyms != null && synonyms.length > 0) {
        allFunctionNames = new String[1 + synonyms.length];
        allFunctionNames[0] = functionName;
        System.arraycopy(synonyms, 0, allFunctionNames, 1, synonyms.length);
      } else {
        allFunctionNames = new String[]{functionName};
      }

      for(String eachFunctionName: allFunctionNames) {
        for (ParamTypes params : paramArray) {
          ParamOptionTypes[] paramOptionArray;
          if(params.paramOptionTypes() == null ||
            params.paramOptionTypes().getClass().getAnnotation(ParamTypes.class) == null) {
            paramOptionArray = new ParamOptionTypes[0];
          } else {
            paramOptionArray = params.paramOptionTypes().getClass().getAnnotation(ParamTypes.class).paramOptionTypes();
          }

          TajoDataTypes.Type[] paramTypes = params.paramTypes();
          if (paramOptionArray.length > 0)
            paramTypes = params.paramTypes().clone();

          for (int i=0; i < paramOptionArray.length + 1; i++) {
            FunctionDesc functionDesc = new FunctionDesc(eachFunctionName,
              function.getClass(), function.getFunctionType(),
              CatalogUtil.newSimpleDataType(returnType),
              paramTypes.length == 0 ? CatalogUtil.newSimpleDataTypeArray() : CatalogUtil.newSimpleDataTypeArray(paramTypes));

            functionDesc.setDescription(description);
            functionDesc.setExample(example);
            functionDesc.setDetail(detail);
            sqlFuncs.add(functionDesc);

            if (i != paramOptionArray.length) {
              paramTypes = new TajoDataTypes.Type[paramTypes.length +
                paramOptionArray[i].paramOptionTypes().length];
              System.arraycopy(params.paramTypes(), 0, paramTypes, 0, paramTypes.length);
              System.arraycopy(paramOptionArray[i].paramOptionTypes(), 0, paramTypes, paramTypes.length,
                paramOptionArray[i].paramOptionTypes().length);
            }
          }
        }
      }
    }

    return sqlFuncs;
  }

  private static void assertJsonSerDer(EvalNode expr) {
    String json = CoreGsonHelper.toJson(expr, EvalNode.class);
    EvalNode fromJson = (EvalNode)CoreGsonHelper.fromJson(json, EvalNode.class);
    Assert.assertEquals(expr, fromJson);
  }

  public TajoConf getConf() {
    return new TajoConf(conf);
  }

  private static Target[] getRawTargets(QueryContext context, String query, boolean condition) throws TajoException, InvalidStatementException {
    List parsedResults = SimpleParser.parseScript(query);
    if(parsedResults.size() > 1) {
      throw new RuntimeException("this query includes two or more statements.");
    } else {
      Expr expr = analyzer.parse(((ParsedResult)parsedResults.get(0)).getHistoryStatement());
      VerificationState state = new VerificationState();
      preLogicalPlanVerifier.verify(context, state, expr);
      if(state.getErrors().size() > 0) {
        if(!condition && state.getErrors().size() > 0) {
          throw new RuntimeException((Throwable)state.getErrors().get(0));
        }

        Assert.assertFalse(((Throwable)state.getErrors().get(0)).getMessage(), true);
      }

      LogicalPlan plan = planner.createPlan(context, expr, true);
      optimizer.optimize(context, plan);
      annotatedPlanVerifier.verify(state, plan);
      if(state.getErrors().size() > 0) {
        Assert.assertFalse(((Throwable)state.getErrors().get(0)).getMessage(), true);
      }

      Target[] targets = plan.getRootBlock().getRawTargets();
      if(targets == null) {
        throw new RuntimeException("Wrong query statement or query plan: " + ((ParsedResult)parsedResults.get(0)).getHistoryStatement());
      } else {
        Target[] var8 = targets;
        int var9 = targets.length;

        int var10;
        Target t;
        for(var10 = 0; var10 < var9; ++var10) {
          t = var8[var10];

          try {
            Assert.assertEquals(t.getEvalTree(), t.getEvalTree().clone());
          } catch (CloneNotSupportedException var13) {
            Assert.fail(var13.getMessage());
          }
        }

        var8 = targets;
        var9 = targets.length;

        for(var10 = 0; var10 < var9; ++var10) {
          t = var8[var10];
          assertJsonSerDer(t.getEvalTree());
        }

        var8 = targets;
        var9 = targets.length;

        for(var10 = 0; var10 < var9; ++var10) {
          t = var8[var10];
          assertEvalTreeProtoSerDer(context, t.getEvalTree());
        }

        return targets;
      }
    }
  }

  public void testSimpleEval(String query, String[] expected) throws TajoException {
    this.testEval((Schema)null, (String)null, (String)null, query, expected);
  }

  public void testSimpleEval(OverridableConf context, String query, String[] expected) throws TajoException {
    this.testEval(context, (Schema)null, (String)null, (String)null, query, expected);
  }

  public void testSimpleEval(String query, String[] expected, boolean successOrFail) throws TajoException, IOException {
    this.testEval((OverridableConf)null, (Schema)null, (String)null, (String)null, query, expected, ',', successOrFail);
  }

  public void testSimpleEval(OverridableConf context, String query, String[] expected, boolean successOrFail) throws TajoException, IOException {
    this.testEval(context, (Schema)null, (String)null, (String)null, query, expected, ',', successOrFail);
  }

  public void testEval(Schema schema, String tableName, String csvTuple, String query, String[] expected) throws TajoException {
    this.testEval((OverridableConf)null, schema, tableName != null?CatalogUtil.normalizeIdentifier(tableName):null, csvTuple, query, expected, ',', true);
  }

  public void testEval(OverridableConf context, Schema schema, String tableName, String csvTuple, String query, String[] expected) throws TajoException {
    this.testEval(context, schema, tableName != null?CatalogUtil.normalizeIdentifier(tableName):null, csvTuple, query, expected, ',', true);
  }

  public void testEval(Schema schema, String tableName, String csvTuple, String query, String[] expected, char delimiter, boolean condition) throws TajoException {
    this.testEval((OverridableConf)null, schema, tableName != null?CatalogUtil.normalizeIdentifier(tableName):null, csvTuple, query, expected, delimiter, condition);
  }

  public void testEval(OverridableConf context, Schema schema, String tableName, String csvTuple, String query, String[] expected, char delimiter, boolean condition) throws TajoException {
    QueryContext queryContext;
    if(context == null) {
      queryContext = LocalTajoTestingUtility.createDummyContext(conf);
    } else {
      queryContext = LocalTajoTestingUtility.createDummyContext(conf);
      queryContext.putAll(context);
    }

    String timezoneId = queryContext.get(SessionVars.TIMEZONE);
    TimeZone timeZone = TimeZone.getTimeZone(timezoneId);
    VTuple vtuple = null;
    String qualifiedTableName = CatalogUtil.buildFQName(new String[]{"default", tableName != null?CatalogUtil.normalizeIdentifier(tableName):null});
    Schema inputSchema = null;
    if(schema != null) {
      inputSchema = SchemaUtil.clone(schema);
      inputSchema.setQualifier(qualifiedTableName);
      int[] targets = new int[inputSchema.size()];

      for(int classLoader = 0; classLoader < targets.length; targets[classLoader] = classLoader++) {
        ;
      }

      byte[][] var38 = BytesUtils.splitPreserveAllTokens(csvTuple.getBytes(), delimiter, targets, inputSchema.size());
      LazyTuple lazyTuple = new LazyTuple(inputSchema, var38, 0L);
      vtuple = new VTuple(inputSchema.size());

      for(int evalContext = 0; evalContext < inputSchema.size(); ++evalContext) {
        Datum outTuple = lazyTuple.get(evalContext);
        boolean e = outTuple instanceof TextDatum || outTuple instanceof CharDatum;
        e = e && outTuple.asChars().equals("") || outTuple.asChars().equals(queryContext.get(SessionVars.NULL_CHAR));
        e |= outTuple.isNull();
        if(e) {
          vtuple.put(evalContext, NullDatum.get());
        } else {
          vtuple.put(evalContext, lazyTuple.get(evalContext));
        }
      }

      try {
        cat.createTable(new TableDesc(qualifiedTableName, inputSchema, "TEXT", new KeyValueSet(), CommonTestingUtil.getTestDir().toUri()));
      } catch (IOException var31) {
        throw new TajoInternalError(var31);
      }
    }

    TajoClassLoader var37 = new TajoClassLoader();
    EvalContext var39 = new EvalContext();

    try {
      Target[] var36 = getRawTargets(queryContext, query, condition);
      EvalCodeGenerator var41 = null;
      if(queryContext.getBool(SessionVars.CODEGEN)) {
        var41 = new EvalCodeGenerator(var37);
      }

      QueryExecutor.startScriptExecutors(queryContext, var39, var36);
      VTuple var40 = new VTuple(var36.length);

      int i;
      for(i = 0; i < var36.length; ++i) {
        EvalNode outTupleAsChars = var36[i].getEvalTree();
        if(queryContext.getBool(SessionVars.CODEGEN)) {
          outTupleAsChars = var41.compile(inputSchema, outTupleAsChars);
        }

        outTupleAsChars.bind(var39, inputSchema);
        var40.put(i, outTupleAsChars.eval(vtuple));
      }

      try {
        var37.clean();
      } catch (Throwable var30) {
        var30.printStackTrace();
      }

      for(i = 0; i < expected.length; ++i) {
        String var42;
        if(var40.type(i) == Type.TIMESTAMP) {
          var42 = TimestampDatum.asChars(var40.getTimeDate(i), timeZone, false);
        } else if(var40.type(i) == Type.TIME) {
          var42 = TimeDatum.asChars(var40.getTimeDate(i), timeZone, false);
        } else {
          var42 = var40.getText(i);
        }

        Assert.assertEquals(query, expected[i], var42);
      }
    } catch (IOException var32) {
      throw new TajoInternalError(var32);
    } catch (InvalidStatementException var33) {
      Assert.assertFalse(var33.getMessage(), true);
    } catch (TajoException var34) {
      if(condition) {
        throw var34;
      }

      Assert.assertEquals(expected[0], var34.getMessage());
    } finally {
      if(schema != null) {
        cat.dropTable(qualifiedTableName);
      }

      QueryExecutor.stopScriptExecutors(var39);
    }
  }

  public static void assertEvalTreeProtoSerDer(OverridableConf context, EvalNode evalNode) {
    EvalNodeTree converted = EvalNodeSerializer.serialize(evalNode);
    Assert.assertEquals(evalNode, EvalNodeDeserializer.deserialize(context, (EvalContext)null, converted));
  }
}
