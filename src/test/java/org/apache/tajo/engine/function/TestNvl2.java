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

import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.exception.UndefinedFunctionException;
import org.junit.Test;

import static org.junit.Assert.fail;

public class TestNvl2 extends ExprTest {
  @Test
  public void testNvl2Text() throws Exception {
    testSimpleEval("select nvl2('value1', 'value2');", new String[]{"value1"});
    testSimpleEval("select nvl2(null, 'value2');", new String[]{"value2"});
    testSimpleEval("select nvl2(null, null, 'value3');", new String[]{"value3"});
    testSimpleEval("select nvl2('value1', null, 'value3');", new String[]{"value1"});
    testSimpleEval("select nvl2(null, 'value2', 'value3');", new String[]{"value2"});
    testSimpleEval("select nvl2('value1');", new String[]{"value1"});

    //no matched function
    try {
      testSimpleEval("select nvl2(null, 2, 'value3');", new String[]{"2"});
      fail("nvl2(NULL, INT, TEXT) not defined. So should throw exception.");
    } catch (UndefinedFunctionException e) {
      //success
    }
  }

  @Test
  public void testNvl2Long() throws Exception {
    testSimpleEval("select nvl2(1, 2);", new String[]{"1"});
    testSimpleEval("select nvl2(null, 2);", new String[]{"2"});
    testSimpleEval("select nvl2(null, null, 3);", new String[]{"3"});
    testSimpleEval("select nvl2(1, null, 3);", new String[]{"1"});
    testSimpleEval("select nvl2(null, 2, 3);", new String[]{"2"});
    testSimpleEval("select nvl2(1);", new String[]{"1"});

    //no matched function
    try {
      testSimpleEval("select nvl2(null, 'value2', 3);", new String[]{"2"});
      fail("nvl2(NULL, TEXT, INT) not defined. So should throw exception.");
    } catch (UndefinedFunctionException e) {
      //success
    }
  }

  @Test
  public void testNvl2Double() throws Exception {
    testSimpleEval("select nvl2(1.0, 2.0);", new String[]{"1.0"});
    testSimpleEval("select nvl2(null, 2.0);", new String[]{"2.0"});
    testSimpleEval("select nvl2(null, null, 3.0);", new String[]{"3.0"});
    testSimpleEval("select nvl2(1.0, null, 3.0);", new String[]{"1.0"});
    testSimpleEval("select nvl2(null, 2.0, 3.0);", new String[]{"2.0"});
    testSimpleEval("select nvl2(1.0);", new String[]{"1.0"});

    //no matched function
    try {
      testSimpleEval("select nvl2('value1', null, 3.0);", new String[]{"1.0"});
      fail("nvl2(TEXT, NULL, FLOAT8) not defined. So should throw exception.");
    } catch (UndefinedFunctionException e) {
      // success
    }

    try {
      testSimpleEval("select nvl2(null, 'value2', 3.0);", new String[]{"2.0"});
      fail("nvl2(NULL, TEXT, FLOAT8) not defined. So should throw exception.");
    } catch (UndefinedFunctionException e) {
      //success
    }
  }

  @Test
  public void testNvl2Boolean() throws Exception {
    testSimpleEval("select nvl2(null, false);", new String[]{"f"});
    testSimpleEval("select nvl2(null, null, true);", new String[]{"t"});
    testSimpleEval("select nvl2(true, null, false);", new String[]{"t"});
    testSimpleEval("select nvl2(null, true, false);", new String[]{"t"});
  }

  @Test
  public void testNvl2Timestamp() throws Exception {
    testSimpleEval("select nvl2(null, timestamp '2014-01-01 00:00:00');",
      new String[]{"2014-01-01 00:00:00"});
    testSimpleEval("select nvl2(null, null, timestamp '2014-01-01 00:00:00');",
      new String[]{"2014-01-01 00:00:00"});
    testSimpleEval("select nvl2(timestamp '2014-01-01 00:00:00', null, timestamp '2014-01-02 00:00:00');",
      new String[]{"2014-01-01 00:00:00"});
    testSimpleEval("select nvl2(null, timestamp '2014-01-01 00:00:00', timestamp '2014-02-01 00:00:00');",
      new String[]{"2014-01-01 00:00:00"});
  }

  @Test
  public void testNvl2Time() throws Exception {
    testSimpleEval("select nvl2(null, time '12:00:00');",
      new String[]{"12:00:00"});
    testSimpleEval("select nvl2(null, null, time '12:00:00');",
      new String[]{"12:00:00"});
    testSimpleEval("select nvl2(time '12:00:00', null, time '13:00:00');",
      new String[]{"12:00:00"});
    testSimpleEval("select nvl2(null, time '12:00:00', time '13:00:00');",
      new String[]{"12:00:00"});
  }

  @Test
  public void testNvl2Date() throws Exception {
    testSimpleEval("select nvl2(null, date '2014-01-01');", new String[]{"2014-01-01"});
    testSimpleEval("select nvl2(null, null, date '2014-01-01');", new String[]{"2014-01-01"});
    testSimpleEval("select nvl2(date '2014-01-01', null, date '2014-02-01');", new String[]{"2014-01-01"});
    testSimpleEval("select nvl2(null, date '2014-01-01', date '2014-02-01');", new String[]{"2014-01-01"});
  }
}
