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

import org.apache.tajo.exception.UndefinedFunctionException;
import org.junit.Test;

public class TestNvl extends ExprTest {
  @Test
  public void testNvlText() throws Exception {
    testSimpleEval("select nvl('value1', 'value2');", new String[]{"value1"});
    testSimpleEval("select nvl(null, 'value2');", new String[]{"value2"});
    testSimpleEval("select nvl('value1', null);", new String[]{"value1"});
    testSimpleEval("select nvl(null, 'value2');", new String[]{"value2"});
  }

  @Test(expected = UndefinedFunctionException.class)
  public void testNvlTextWithWrongParameter1() throws Exception {
    testSimpleEval("select nvl(null, null, 'value3');", new String[]{"value3"});
  }

  @Test(expected = UndefinedFunctionException.class)
  public void testNvlTextWithWrongParameter2() throws Exception {
    testSimpleEval("select nvl('value1');", new String[]{"value1"});
  }

  @Test
  public void testNvlLong() throws Exception {
    testSimpleEval("select nvl(1, 2);", new String[]{"1"});
    testSimpleEval("select nvl(null, 2);", new String[]{"2"});
    testSimpleEval("select nvl(1, null);", new String[]{"1"});
  }

  @Test(expected = UndefinedFunctionException.class)
  public void testNvlLongWithWrongParameter1() throws Exception {
    testSimpleEval("select nvl(null, 2, 3);", new String[]{"2"});
  }

  @Test(expected = UndefinedFunctionException.class)
  public void testNvlLongWithWrongParameter2() throws Exception {
    testSimpleEval("select nvl(1);", new String[]{"1"});
  }

  @Test
  public void testNvlDouble() throws Exception {
    testSimpleEval("select nvl(1.0, 2.0);", new String[]{"1.0"});
    testSimpleEval("select nvl(null, 2.0);", new String[]{"2.0"});
    testSimpleEval("select nvl(1.0, null);", new String[]{"1.0"});
  }

  @Test(expected = UndefinedFunctionException.class)
  public void testNvlDoubleWithWrongParameter1() throws Exception {
    testSimpleEval("select nvl(null, 2.0, 3.0);", new String[]{"2.0"});
  }

  @Test(expected = UndefinedFunctionException.class)
  public void testNvlDoubleWithWrongParameter2() throws Exception {
    testSimpleEval("select nvl(1.0);", new String[]{"1.0"});
  }

  @Test
  public void testNvlBoolean() throws Exception {
    testSimpleEval("select nvl(null, false);", new String[]{"f"});
    testSimpleEval("select nvl(true, null);", new String[]{"t"});
    testSimpleEval("select nvl(null, true);", new String[]{"t"});
  }

  @Test(expected = UndefinedFunctionException.class)
  public void testNvlBooleanWithWrongParameter() throws Exception {
    testSimpleEval("select nvl(null, true, false);", new String[]{"t"});
  }

  @Test
  public void testNvlTimestamp() throws Exception {
    testSimpleEval("select nvl(null, timestamp '2016-03-01 00:00:00');",
      new String[]{"2016-03-01 00:00:00"});
    testSimpleEval("select nvl(timestamp '2016-03-01 00:00:00', timestamp '2016-03-02 00:00:00');",
      new String[]{"2016-03-01 00:00:00"});
    testSimpleEval("select nvl(timestamp '2016-03-01 00:00:00', null);",
      new String[]{"2016-03-01 00:00:00"});
  }

  @Test(expected = UndefinedFunctionException.class)
  public void testNvlTimestampWithWrongParameter() throws Exception {
    testSimpleEval("select nvl(null, timestamp '2016-03-01 00:00:00', timestamp '2016-02-01 00:00:00');",
      new String[]{"2016-03-01 00:00:00"});
  }

  @Test
  public void testNvlTime() throws Exception {
    testSimpleEval("select nvl(null, time '12:00:00');",
      new String[]{"12:00:00"});
    testSimpleEval("select nvl(time '12:00:00', null);",
      new String[]{"12:00:00"});
  }

  @Test(expected = UndefinedFunctionException.class)
  public void testNvlTimeWithWrongParameter() throws Exception {
    testSimpleEval("select nvl(null, time '12:00:00', time '13:00:00');",
      new String[]{"12:00:00"});
  }

  @Test
  public void testNvlDate() throws Exception {
    testSimpleEval("select nvl(null, date '2016-03-01');", new String[]{"2016-03-01"});
    testSimpleEval("select nvl(date '2016-02-01', date '2016-03-01');", new String[]{"2016-02-01"});
  }

  @Test(expected = UndefinedFunctionException.class)
  public void testNvlDateWithWrongParameter() throws Exception {
    testSimpleEval("select nvl(null, date '2016-03-01', date '2016-02-01');", new String[]{"2016-03-01"});
  }
}
