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

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;

@Description(
  functionName = "nvl2",
  description = "Returns the first of its arguments that is not null.",
  detail = "Like a CASE expression, NVL2 only evaluates the arguments that are needed to determine the result; " +
    "that is, arguments to the right of the first non-null argument are not evaluated",
  example = "> SELECT nvl2(null, null, true);\n"
    + "true",
  returnType = TajoDataTypes.Type.BOOLEAN,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.BOOLEAN, TajoDataTypes.Type.BOOLEAN_ARRAY})}
)
public class Nvl2Boolean extends Nvl2 {
  public Nvl2Boolean() {
    super(new Column[] {
      new Column("param", TajoDataTypes.Type.BOOLEAN),
      new Column("params", TajoDataTypes.Type.BOOLEAN_ARRAY),
    });
  }

}
