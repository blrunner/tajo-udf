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

import com.blrunner.tajo.udf.Nvl;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;

@Description(
  functionName = "nvl",
  description = "If expr1 is null, then NVL returns expr2. If expr1 is not null, then NVL returns expr1.",
  example = "> SELECT nvl(dept, 'Not Applicable') FROM src;\n" +
    " 'Not Applicable' if dept is null\n",
  returnType = TajoDataTypes.Type.FLOAT8,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT8, TajoDataTypes.Type.FLOAT8})}
)
public class NvlDouble extends Nvl {
  public NvlDouble() {
    super(new Column[] {
      new Column("expr1", TajoDataTypes.Type.FLOAT8),
      new Column("expr2", TajoDataTypes.Type.FLOAT8)
    });
  }
}

