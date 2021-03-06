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
  functionName = "greatest",
  description = "Returns the greatest value in the list.",
  example = "> SELECT greatest(2.0, 5.0, 12.0, 3.0) FROM src;\n 12.0",
  returnType = TajoDataTypes.Type.FLOAT8,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT8_ARRAY})}
)
public class GreatestDouble extends Greatest {
  public GreatestDouble() {
    super(new Column[] {
      new Column("values", TajoDataTypes.Type.FLOAT8_ARRAY)
    });
  }
}