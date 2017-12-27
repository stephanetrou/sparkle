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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.eventbus.EventBus;
import my.little.pony.sparkle.listener.event.JobEnd;
import my.little.pony.sparkle.listener.JobExecutionListener;
import my.little.pony.sparkle.listener.event.JobStart;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JobContext {

    private Class<?> jobClass;
    private Map<String, Dataset<Row>> datasets;
    private Function<String, String> destination = s -> s;
    private String previousKey;
    private EventBus eventBus;
    private List<String> packages;

    public JobContext() {
        eventBus = new EventBus();
        datasets = new ConcurrentHashMap<>();
        packages = new ArrayList<>();
    }

    public JobContext(Class<?> jobClass) {
        this();
        this.jobClass = jobClass;
        addPackages(jobClass.getPackage());
    }

    public void ds(String name, Dataset<Row> dataset) {
        datasets.put(name, dataset);
        previousKey = name;
    }

    public Dataset<Row> ds(String name) {
        if(!datasets.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Unknown dataset %s", name));
        }

        return datasets.get(name);
    }

    public Dataset<Row> previous() {
        return ds(previousKey);
    }


    public void addPackages(Package _package) {

         packages.add(_package.getName());
    }

    public void addListener(JobExecutionListener jobExecutionListener) {
         eventBus.register(jobExecutionListener);
    }

    public void fireJobStart() {
        eventBus.post(new JobStart(this));
    }

    public void fireJobEnd() {
        eventBus.post(new JobEnd());
    }


    public Class<?> getJobClass() {
        return jobClass;
    }

    public void setJobClass(Class<?> jobClass) {
        this.jobClass = jobClass;
    }

    public void destination(Function<String, String> destination) {
        Preconditions.checkNotNull(destination, "Function destination should not be null");
        this.destination = destination;
    }

    public Function<String, String>  destination() {
        return destination;
    }


    public List<String> getPackages() {
        return packages;
    }


}



