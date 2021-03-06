/*
 * Copyright 2017 stephanetrou
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package my.little.pony.sparkle.sample.job.udf;

import my.little.pony.sparkle.udf.UDF;
import my.little.pony.sparkle.udf.UDFReturnType;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF1;

@UDF(name="is_not_blank", type = UDFReturnType.Boolean)
public class IsNotBlank implements UDF1<String, Boolean> {
    
    @Override
    public Boolean call(String s) {
        return StringUtils.isNotBlank(s);
    }
}
