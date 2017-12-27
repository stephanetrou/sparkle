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

package my.little.pony.sparkle.catalog;

import java.lang.reflect.Field;

import static my.little.pony.sparkle.Sparkle.spark;
import my.little.pony.sparkle.catalog.MyCatalog;
import my.little.pony.sparkle.job.JobContext;
import my.little.pony.sparkle.spi.Register;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;

public class CatologRegister implements Register {
    
    @Override
    public void register(JobContext jobContext) {
        try {
            Field externalCatalog = FieldUtils.getField(SessionCatalog.class, "externalCatalog", true);
            FieldUtils.removeFinalModifier(externalCatalog, true);
            externalCatalog.set(spark().sessionState().catalog(), new MyCatalog(spark().sessionState().catalog().externalCatalog()));
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }
}
