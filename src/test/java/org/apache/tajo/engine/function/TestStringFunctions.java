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

public class TestStringFunctions extends ExprTest {

  @Test
  public void testReplace() throws Exception {
    testSimpleEval("select replace('JACK and JUE','J','BL');", new String[]{"BLACK and BLUE"});
    testSimpleEval("select replace('tajo1234', 'jo', 'abc');", new String[]{"taabc1234"});
    testSimpleEval("select replace('value1', null, 'value2');", new String[]{"value1"});
  }

}
