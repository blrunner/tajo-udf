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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;


/**
 * UDF for string function <code>LAST_DAY()</code>,
 * <a href="http://docs.oracle.com/cd/B28359_01/server.111/b28286/functions179.htm">LAST_DAY</a>.
 *
 */
@Description(
  functionName = "last_day",
  description = "returns the last day of the month based on a date string with YYYY-MM-DD HH24:MI:SS pattern",
  example = "> SELECT last_day(current_date()) FROM src;\n" +
    " SELECT last_day(cast('2016-03-29 17:10:00' as date)') FROM src;\n",
  returnType = TajoDataTypes.Type.DATE,
  paramTypes = {
    @ParamTypes(paramTypes = {TajoDataTypes.Type.DATE})
  }
)

public class LastDay extends GeneralFunction {
  private final SimpleDateFormat standardFormatter = new SimpleDateFormat("yyyy-MM-dd");

  @Expose
  private TimeZone timezone;

  public LastDay() {
    super(new Column[]{
      new Column("date", TajoDataTypes.Type.DATE)
    });
  }

  @Override
  public void init(OverridableConf context, FunctionEval.ParamType[] types) {
    String timezoneId = context.get(SessionVars.TIMEZONE, "GMT");
    this.timezone = TimeZone.getTimeZone(timezoneId);
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0)) {
      return NullDatum.get();
    }

    TimeMeta tm = params.getTimeDate(0);
    DateTimeUtil.toUserTimezone(tm, this.timezone);

    Calendar calendar = Calendar.getInstance(this.timezone);
    try {
      calendar.setTime(standardFormatter.parse(
        DatumFactory.createText(DateTimeFormat.to_char(tm, "YYYY-MM-DD")).asChars()));
    } catch (ParseException e) {
      e.printStackTrace();
    }

    int lastDate = calendar.getActualMaximum(Calendar.DATE);

    return DatumFactory.createDate(tm.years, tm.monthOfYear, lastDate);
  }
}