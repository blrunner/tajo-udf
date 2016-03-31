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
 * Abstract UDF Class for SQL construct "greatest(value1, value2, value3, ....)".
 * Oracle's <a href="http://docs.oracle.com/cd/B28359_01/server.111/b28286/functions062.htm#SQLRF00645">GREATEST</a>
 * returns the greatest of the list of one or more expressions.
 *
 */
abstract class Greatest extends GeneralFunction {
  public Greatest(Column[] definedArgs) {
    super(definedArgs);
  }

  @Override
  public Datum eval(Tuple params) {
    Datum greatestDatum = params.asDatum(0);

    if (params.size() == 1) {
      return greatestDatum;
    }

    for (int i = 1; i < params.size(); i++) {
      Datum datum = params.asDatum(i);
      if (datum.compareTo(greatestDatum) > 0) {
        greatestDatum = datum;
      }
    }

    return greatestDatum;
  }
}
