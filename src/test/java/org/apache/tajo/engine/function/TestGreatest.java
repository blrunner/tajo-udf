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

import org.junit.Test;

public class TestGreatest extends ExprTest {
  @Test
  public void testGreatestInt() throws Exception {
    testSimpleEval("select greatest(2, 5, 12, 3)", new String[]{"12"});
    testSimpleEval("select greatest(5);", new String[]{"5"});
    testSimpleEval("select greatest(100, 300, 600, 1000, 2000, 10000, 5000);",
      new String[]{"10000"});
  }

  @Test
  public void testGreatestLong() throws Exception {
    testSimpleEval("select greatest(2147483650);", new String[]{"2147483650"});
    testSimpleEval("select greatest(2147483647, 2147483650, 2147483747, 2147483547);", new String[]{"2147483747"});
    testSimpleEval("select greatest(214748364700, 214748368000, 214748374700, 214748354700);",
      new String[]{"214748374700"});
  }

  @Test
  public void testGreatestFloat() throws Exception {
    testSimpleEval("select greatest(2.0, 5.0, 12.0, 3.0)", new String[]{"12.0"});
    testSimpleEval("select greatest(5.0);", new String[]{"5.0"});
    testSimpleEval("select greatest(100.0, 300.0, 600.0, 1000.0, 2000.0, 10000.0, 5000.0);",
      new String[]{"10000.0"});
  }

  @Test
  public void testGreatestDouble() throws Exception {
    testSimpleEval("select greatest(1.7976931348623, 1.7976931348625, 1.7976931348699)",
      new String[]{"1.7976931348699"});
    testSimpleEval("select greatest(1.7976931348623);", new String[]{"1.7976931348623"});
  }

  @Test
  public void testGreatestText() throws Exception {
    testSimpleEval("select greatest('2', '5', '12', '3')", new String[]{"5"});
    testSimpleEval("select greatest('tajo');", new String[]{"tajo"});
    testSimpleEval("select greatest('apples', 'oranges', 'bananas');", new String[]{"oranges"});
  }
}
