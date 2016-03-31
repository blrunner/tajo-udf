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
import org.apache.tajo.datum.Datum;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;

/**
 * Abstract UDF Class for SQL construct "nvl(expr1, expr2)". see <a href=
 * "http://download.oracle.com/docs/cd/B19306_01/server.102/b14200/functions105.htm" >NVL</a>.
 * <p>
 * There is function <code>COALESCE</code> in Tajo,
 * but it is convenient to convert from Oracle SQL to Tajo SQL without query
 * changes.
 * <p>
 *
 */
abstract class Nvl extends GeneralFunction {
  public Nvl(Column[] definedArgs) {
    super(definedArgs);
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0)) {
      return params.asDatum(1);
    } else {
      return params.asDatum(0);
    }
  }
}

