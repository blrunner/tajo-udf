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

package com.blrunner.tajo.udf;

import com.google.gson.annotations.Expose;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.expr.FunctionEval;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.datetime.DateTimeFormat;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import java.util.TimeZone;

/**
 * UDF for string function <code>CURDATE()</code>,
 * <code>SYSDATE()</code>. This mimcs the function from MySQL
 * http://dev.mysql.com/doc/refman/5.1/en/date-and-time-functions.html#function_sysdate
 *
 * <pre>
 * usage:
 * SYSDATE()
 * </pre>
 * <p>
 */
@Description(
  functionName = "sysdate",
  description = "sysdate() - Returns the current date and time as a value in 'yyyy-MM-dd HH:mm:ss' format"
    +" sysdate(dateFormat) - Returns the current date and time as a value in given format"
    +" sysdate(dateFormat, num_days) - Returns the date that is num_days after current date in given date format",
  example = "> SELECT sysdate();\n2016-04-01"+ "  > SELECT sysdate('yyyyMMdd') FROM src LIMIT 1;\n" + "20160401"
    + "  > SELECT sysdate('yyyyMMdd',1) FROM src LIMIT 1;\n" + "20160402",
  returnType = TajoDataTypes.Type.TEXT,
  paramTypes = {
    @ParamTypes(paramTypes = {}),
    @ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT}),
    @ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.INT4}),
    @ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.INT8})
  }
)
public class SysDate extends GeneralFunction {
  private final String DEFAULT_FORMAT = "YYYY-MM-DD HH24:MI:SS";

  @Expose
  private TimeZone timezone;
  private TextDatum datum;

  public SysDate() {
    super(new Column[]{
      new Column("pattern", TajoDataTypes.Type.TEXT),
      new Column("num_days", TajoDataTypes.Type.INT4)
    });
  }

  @Override
  public void init(OverridableConf context, FunctionEval.ParamType[] types) {
    String timezoneId = context.get(SessionVars.TIMEZONE, "GMT");
    this.timezone = TimeZone.getTimeZone(timezoneId);
    System.out.println("### timezone:" + timezone);
  }

  @Override
  public Datum eval(Tuple params) {
    int paramsSize = params.size();

    if (this.datum == null) {
      long julianTimestamp = DateTimeUtil.javaTimeToJulianTime(System.currentTimeMillis());
      TimeMeta tm = new TimeMeta();
      DateTimeUtil.toJulianTimeMeta(julianTimestamp, tm);
      DateTimeUtil.toUserTimezone(tm, this.timezone);

      if (paramsSize == 0) {
        this.datum = DatumFactory.createText(DateTimeFormat.to_char(tm, DEFAULT_FORMAT));
      } else if (paramsSize == 1){
        String pattern = params.getText(0);
        this.datum = DatumFactory.createText(DateTimeFormat.to_char(tm, pattern));
      } else if (paramsSize == 2){
        String pattern = params.getText(0);
        long numDays = params.getInt8(1);
        TimeMeta finalTm = null;

        // Get current date
        DateDatum dateDatum = DatumFactory.createDate(DateTimeFormat.to_char(tm, pattern));

        // Add days
        Datum resultDatum = null;
        if (numDays >= 0) {
          resultDatum = dateDatum.plus(new IntervalDatum(numDays * IntervalDatum.DAY_MILLIS));
        } else {
          resultDatum = dateDatum.minus(new IntervalDatum(0 - numDays * IntervalDatum.DAY_MILLIS));
        }

        // Create final value
        if (resultDatum instanceof DateDatum) {
          finalTm = DatumFactory.createDate(resultDatum).asTimeMeta();
        } else if (resultDatum instanceof TimestampDatum) {
          finalTm = new TimeMeta();
          julianTimestamp = ((TimestampDatum) resultDatum).getTimestamp();
          DateTimeUtil.toJulianTimeMeta(julianTimestamp, finalTm);
        }
        this.datum = DatumFactory.createText(DateTimeFormat.to_char(finalTm, pattern));
      }
    }

    return this.datum;
  }
}