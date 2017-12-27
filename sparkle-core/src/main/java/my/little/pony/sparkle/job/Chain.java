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

package my.little.pony.sparkle.job;

import java.util.function.BiConsumer;

import static my.little.pony.sparkle.io.SparkleFormat.none;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;

public class Chain {

    private JobContext context;

    private Chain(JobContext context) {
        this.context = context;
    }

    public static Chain withContext(JobContext context) {
        return new Chain(context);
    }

    public Chain step(Class<? extends SubJob> subjob) {
        return step(subjob, none());
    }

    public Chain step(Class<? extends SubJob> subjob, BiConsumer<DataFrameWriter<Row>, JobContext> biConsumer) {
        return step(subjob.getSimpleName(), subjob, biConsumer);
    }

    public Chain step(String name, Class<? extends SubJob> subjob, BiConsumer<DataFrameWriter<Row>, JobContext> biConsumer) {
        return this;
    }

    public void run() {
        
    }

}
