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


import java.util.Arrays;

import com.google.common.reflect.TypeToken;
import my.little.pony.sparkle.cli.CliOptions;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import my.little.pony.sparkle.Sparkle;

public abstract class Job<T extends CliOptions> {

    private TypeToken<T> cliOptionsClass = new TypeToken<T>(getClass()) { };

    private String[] source;
    private String destination;
    private String[] columns;
        
    private boolean debug;

    public String[] getSource() {
        return source;
    }

    public String getDestination() {
        return destination;
    }

    public abstract void run();

    public Class<T> getCliOptions() {
        return (Class<T>) cliOptionsClass.getRawType();
    }

    public Dataset<Row> source() {
        Sparkle.description(this.getClass().getSimpleName(), "Read : " + Arrays.toString(source));
        return Sparkle.spark().read().parquet(source);
    }

    public void destination(Dataset<Row> ds, SaveMode saveMode) {
        Sparkle.description(this.getClass().getSimpleName(), "Writing : " + destination);

        DataFrameWriter<Row> writer = ds.write().mode(saveMode);

        if (columns != null && columns.length > 0) {
            writer = writer.partitionBy(columns);
        }

        int numPartitions = ds.rdd().getNumPartitions();

        
        writer.parquet(destination);
    }
}
