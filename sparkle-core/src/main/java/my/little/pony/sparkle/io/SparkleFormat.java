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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static my.little.pony.sparkle.Sparkle.description;
import com.google.common.base.Preconditions;
import my.little.pony.sparkle.job.JobContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkleFormat {

    public static BiFunction<DataFrameReader, JobContext, Dataset<Row>> parquet(String... name) {
        Preconditions.checkState(StringUtils.isNoneBlank(name), "Name should not be null");
        description("Reading", String.format("parquet : %s", Arrays.toString(name)));

        return (dfr, context) ->
                dfr.parquet(source(context, name));
    }

    public static BiFunction<DataFrameReader, JobContext, Dataset<Row>> orc(String... name) {
        Preconditions.checkState(StringUtils.isNoneBlank(name), "Name should not be null");
        description("Reading", String.format("orc : %s", Arrays.toString(name)));

        return (dfr, context) ->
                dfr.orc(source(context, name));
    }

    public static BiFunction<DataFrameReader, JobContext, Dataset<Row>> text(String... name) {
        Preconditions.checkState(StringUtils.isNoneBlank(name), "Name should not be null");
        description("Reading", String.format("text : %s", Arrays.toString(name)));

        return (dfr, context) ->
                dfr.text(source(context, name));

    }

    public static BiFunction<DataFrameReader, JobContext, Dataset<Row>> csv(String... name) {
        Preconditions.checkState(StringUtils.isNoneBlank(name), "Name should not be null");
        description("Reading", String.format("csv : %s", Arrays.toString(name)));

        return (dfr, context) ->
                dfr.options(options(false, ",")).csv(source(context, name));

    }

    public static BiFunction<DataFrameReader, JobContext, Dataset<Row>> csvh(String... name) {
        Preconditions.checkState(StringUtils.isNoneBlank(name), "Name should not be null");
        description("Reading", String.format("csvh : %s", Arrays.toString(name)));

        return (dfr, context) ->
                dfr.options(options(true, ",")).csv(source(context, name));

    }

    public static BiFunction<DataFrameReader, JobContext, Dataset<Row>> tsv(String... name) {
        Preconditions.checkState(StringUtils.isNoneBlank(name), "Name should not be null");
        description("Reading", String.format("tsv : %s", Arrays.toString(name)));

        return (dfr, context) ->
                dfr.options(options(false, "\t")).csv(source(context, name));

    }

    public static BiFunction<DataFrameReader, JobContext, Dataset<Row>> tsvh(String... name) {
        Preconditions.checkState(StringUtils.isNoneBlank(name), "Name should not be null");
        description("Reading", String.format("tsvh : %s", Arrays.toString(name)));

        return (dfr, context) ->
                dfr.options(options(true, "\t")).csv(source(context, name));

    }

    public static BiFunction<DataFrameReader, JobContext, Dataset<Row>> ssv(String... name) {
        Preconditions.checkState(StringUtils.isNoneBlank(name), "Name should not be null");
        description("Reading", String.format("ssv : %s", Arrays.toString(name)));

        return (dfr, context) ->
                dfr.options(options(false, ";")).csv(source(context, name));

    }

    public static BiFunction<DataFrameReader, JobContext, Dataset<Row>> ssvh(String... name) {
        Preconditions.checkState(StringUtils.isNoneBlank(name), "Name should not be null");
        description("Reading", String.format("ssvh : %s", Arrays.toString(name)));

        return (dfr, context) ->
                dfr.options(options(true, ";")).csv(source(context, name));

    }

    public static BiFunction<DataFrameReader, JobContext, Dataset<Row>> table(String... name) {
        Preconditions.checkState(StringUtils.isNoneBlank(name), "Name should not be null");
        description("Reading", String.format("table : %s", name[0]));

        return (dfr, context) ->
                dfr.table(name[0]);

    }

    public static BiConsumer<DataFrameWriter<Row>, JobContext> none() {
        return (ds, context) -> {};
    }

    public static BiConsumer<DataFrameWriter<Row>, JobContext> parquet(String name) {
        Preconditions.checkState(StringUtils.isNotBlank(name), "Name should not be null");
        description("Writing", String.format("parquet : %s", name));

        return (ds, context) -> ds.parquet(destination(name, context));
    }

    public static BiConsumer<DataFrameWriter<Row>, JobContext> orc(String name) {
        Preconditions.checkState(StringUtils.isNotBlank(name), "Name should not be null");
        description("Writing", String.format("orc : %s", name));

        return (ds, context) -> ds.orc(destination(name, context));
    }

    public static BiConsumer<DataFrameWriter<Row>, JobContext> text(String name) {
        Preconditions.checkState(StringUtils.isNotBlank(name), "Name should not be null");
        description("Writing", String.format("text : %s", name));

        return (ds, context) -> ds.text(destination(name, context));
    }

    public static BiConsumer<DataFrameWriter<Row>, JobContext> csv(String name) {
        Preconditions.checkState(StringUtils.isNotBlank(name), "Name should not be null");
        description("Writing", String.format("csv : %s", name));

        return (ds, context) -> ds.options(options(false, ",")).csv(destination(name, context));
    }

    public static BiConsumer<DataFrameWriter<Row>, JobContext> csvh(String name) {
        Preconditions.checkState(StringUtils.isNotBlank(name), "Name should not be null");
        description("Writing", String.format("csvh : %s", name));

        return (ds, context) -> ds.options(options(true, ",")).csv(destination(name, context));
    }

    public static BiConsumer<DataFrameWriter<Row>, JobContext> tsv(String name) {
        Preconditions.checkState(StringUtils.isNotBlank(name), "Name should not be null");
        description("Writing", String.format("tsv : %s", name));

        return (ds, context) -> ds.options(options(false, "\t")).csv(destination(name, context));
    }

    public static BiConsumer<DataFrameWriter<Row>, JobContext> tsvh(String name) {
        Preconditions.checkState(StringUtils.isNotBlank(name), "Name should not be null");
        description("Writing", String.format("tsvh : %s", name));

        return (ds, context) -> ds.options(options(true, "\t")).csv(destination(name, context));
    }

    public static BiConsumer<DataFrameWriter<Row>, JobContext> ssv(String name) {
        Preconditions.checkState(StringUtils.isNotBlank(name), "Name should not be null");
        description("Writing", String.format("ssv : %s", name));

        return (ds, context) -> ds.options(options(false, ";")).csv(destination(name, context));
    }

    public static BiConsumer<DataFrameWriter<Row>, JobContext> ssvh(String name) {
        Preconditions.checkState(StringUtils.isNotBlank(name), "Name should not be null");
        description("Writing", String.format("ssvh : %s", name));

        return (ds, context) -> ds.options(options(true, ";")).csv(destination(name, context));
    }

    public static BiConsumer<DataFrameWriter<Row>, JobContext> table(String name) {
        Preconditions.checkState(StringUtils.isNotBlank(name), "Name should not be null");
        description("Writing", String.format("table : %s", name));

        return (ds, context) -> ds.saveAsTable(name);
    }

    private static String destination(String name, JobContext context) {
        return context.destination().apply(name);
    }
        
    private static String[] source(JobContext context, String... name) {
        return name;
    }

    private static Map<String, String> options(boolean header, String delimiter) {
        Map<String, String> options = new HashMap<>();
        options.put("header", String.valueOf(header));
        options.put("delimiter", delimiter);
        return  options;
    }

}
