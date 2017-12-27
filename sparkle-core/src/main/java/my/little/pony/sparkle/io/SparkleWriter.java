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

import java.util.function.BiConsumer;

import my.little.pony.sparkle.job.JobContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class SparkleWriter {

    public static As overwrite(Dataset<Row> ds) {
        return new As(ds.write().mode(SaveMode.Overwrite));
    }

    public static As append(Dataset<Row> ds) {
        return new As(ds.write().mode(SaveMode.Append));
    }

    public static As write(Dataset<Row> ds) {
        return new As(ds.write());
    }


    public static As debug(Dataset<Row> ds) {
        return new As(ds.write().mode(SaveMode.Overwrite));
    }


    public static class As {

        private DataFrameWriter<Row> dfw;


        protected As(DataFrameWriter<Row> dfw) {
            this.dfw = dfw;
        }

        public void as(BiConsumer<DataFrameWriter<Row>, JobContext> biConsumer) {
            biConsumer.accept(dfw, new JobContext());
        }

    }


}
