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

package my.little.pony.sparkle.io;

import java.util.function.BiFunction;

import static my.little.pony.sparkle.Sparkle.spark;
import my.little.pony.sparkle.job.JobContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkleReader {

    public static As read() {
        return new As(spark().read());
    }
    
    public static class As {

        private DataFrameReader dfr;


        protected As(DataFrameReader dfr) {
            this.dfr = dfr;
        }

        public Dataset<Row> as(BiFunction<DataFrameReader, JobContext, Dataset<Row>> biConsumer) {
            return biConsumer.apply(dfr, new JobContext());
        }

    }


}
