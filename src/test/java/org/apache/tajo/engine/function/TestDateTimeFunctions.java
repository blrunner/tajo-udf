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

import org.apache.tajo.SessionVars;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoException;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class TestDateTimeFunctions extends ExprTest {

  @Test
  public void testSysDate() throws TajoException {
    TimeZone originalTimezone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT-6"));

    QueryContext context = new QueryContext(getConf());
    context.put(SessionVars.TIMEZONE, "GMT-6");

    try {
      Date expectedDate = new Date(System.currentTimeMillis());

      testSimpleEval(context, "select sysdate('YYYY-MM-DD HH24:MI');",
        new String[]{dateFormat(expectedDate, "yyyy-MM-dd HH:mm")});

      testSimpleEval(context, "select sysdate('yyyy-MM-dd');",
        new String[]{dateFormat(expectedDate, "yyyy-MM-dd")});

      expectedDate.setDate(expectedDate.getDate() + 1);

      testSimpleEval(context, "select sysdate('yyyy-MM-dd', 1);",
        new String[]{dateFormat(expectedDate, "yyyy-MM-dd")});
    } finally {
      TimeZone.setDefault(originalTimezone);
    }
  }

  private String dateFormat(Date date, String format) {
    SimpleDateFormat df = new SimpleDateFormat(format);
    return df.format(date);
  }

}
