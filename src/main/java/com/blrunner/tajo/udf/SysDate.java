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

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.engine.function.datetime.CurrentDate;

/**
 * UDF for string function <code>CURDATE()</code>,
 * <code>SYSDATE()</code>. This mimcs the function from MySQL
 * http://dev.mysql.com/doc/refman/5.1/en/date-and-time-functions.html#function_sysdate
 *
 * <pre>
 * usage:
 * SYSDATE()
 * </pre>
 * <p>
 */
@Description(
  functionName = "sysdate",
  description = "Get current date. Result is DATE type.",
  example = "> SELECT sysdate();\n2016-04-01",
  returnType = TajoDataTypes.Type.DATE,
  paramTypes = {    @ParamTypes(
    paramTypes = {}
  )}
)
public class SysDate extends CurrentDate {

}
