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
import com.google.common.base.Preconditions;
import my.little.pony.sparkle.job.JobContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkleReader {

    public static As read(String... names) {
        StringUtils.isNoneBlank(names);
        return new As(spark().read(), names);
    }
    

    public final static class As {

        private DataFrameReader dfr;
        private String[] names;

        protected As(DataFrameReader dfr, String... names) {
            this.dfr = dfr;
            this.names = names;
        }

        public Dataset<Row> as(ReaderFunction<DataFrameReader, String[], JobContext, Dataset<Row>> readerFunction) {
            return readerFunction.apply(dfr, names, new JobContext());
        }

    }


}
