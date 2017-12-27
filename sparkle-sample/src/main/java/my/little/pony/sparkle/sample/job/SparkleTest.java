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

package my.little.pony.sparkle.sample.job;

import java.io.IOException;
import java.util.Arrays;

import static my.little.pony.sparkle.Sparkle.description;
import static my.little.pony.sparkle.Sparkle.spark;
import static my.little.pony.sparkle.io.SparkleFormat.csvh;
import static my.little.pony.sparkle.io.SparkleFormat.parquet;
import static my.little.pony.sparkle.io.SparkleReader.read;
import static my.little.pony.sparkle.io.SparkleWriter.overwrite;
import my.little.pony.sparkle.SparkleApplication;
import my.little.pony.sparkle.job.Job;
import my.little.pony.sparkle.sample.job.cli.CliOption2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkleTest extends Job<CliOption2> {

    public static void main(String[] args) {
        SparkleApplication.run(SparkleTest.class, new String[] {
                "-s","/Users/stephanetrou/projets/titanic/train.csv",
                "-d","/tmp/destination.parquet",
                "--debug"
        });
    }

    public void run() {

        read().as(csvh(getSource())).createOrReplaceTempView("train");

        description("query", "show \uD83E\uDD84");
        Dataset<Row> ds = spark().sql("SELECT sum(age) as sum_age, avg(age) as avg_age FROM train where is_not_blank(Cabin) group by sex");

        overwrite(ds).as(parquet(getDestination()));
    }

}
