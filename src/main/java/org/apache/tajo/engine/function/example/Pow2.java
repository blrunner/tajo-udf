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

package org.apache.tajo.engine.function.example;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

import static org.apache.tajo.common.TajoDataTypes.Type.FLOAT8;

/**
 * This is simple example for newbies and it returns x raised to the power of y.
 */
@Description(
  functionName = "pow2",
  description = "x raised to the power of y",
  example = "> SELECT pow2(9.0, 3.0)\n"
    + "729",
  returnType = FLOAT8,
  paramTypes = {
    @ParamTypes(paramTypes = {FLOAT8, FLOAT8})
  }
)
public class Pow2 extends GeneralFunction {
  public Pow2() {
    super(new Column[] {
      new Column("x", FLOAT8),
      new Column("y", FLOAT8)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0) || params.isBlankOrNull(1)) {
      return NullDatum.get();
    }

    return DatumFactory.createFloat8(Math.pow(params.getFloat8(0), params.getFloat8(1)));
  }
}
