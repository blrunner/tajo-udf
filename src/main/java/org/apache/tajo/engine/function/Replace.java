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
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;

/**
 * UDF for string function <code>REPLACE()</code>,
 * <a href="http://docs.oracle.com/cd/B28359_01/server.111/b28286/functions141.htm">REPLACE</a>.
 *
 */
@Description(
  functionName = "replace",
  description = "returns char with every occurrence of search_string replaced with replacement_string",
  example = "> SELECT replace('tajo1234', 'jo', 'abc') FROM src;\n",
  returnType = TajoDataTypes.Type.TEXT,
  paramTypes = {@ParamTypes(paramTypes
    = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT})}
)
public class Replace extends GeneralFunction {
  public Replace() {
    super(new Column[]{
      new Column("char", TajoDataTypes.Type.TEXT),
      new Column("search_string", TajoDataTypes.Type.TEXT),
      new Column("replacement_string", TajoDataTypes.Type.TEXT)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(1)) {
      return DatumFactory.createText(params.getText(0));
    }
    return DatumFactory.createText(params.getText(0).replaceAll(params.getText(1), params.getText(2)));
  }
}