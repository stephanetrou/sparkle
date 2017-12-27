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

package my.little.pony.sparkle.runner;

import java.util.LinkedHashMap;
import java.util.Map;

import my.little.pony.sparkle.job.Chain;
import my.little.pony.sparkle.job.JobContext;
import my.little.pony.sparkle.job.SubJob;
import my.little.pony.sparkle.job.Job;

public abstract class SubJobRunner<T> extends Job {

    private Map<String, Class<SubJob>> flow;

    public abstract void register();

    @Override
    public final void run() {
        Chain chain = Chain.withContext(new JobContext());

        flow = new LinkedHashMap<>();
        register();

        for(Map.Entry<String, Class<SubJob>> entry : flow.entrySet()) {
            System.out.println("Running " + entry.getKey());
        }
    }
    

    public <C> void next(String subJobName, Class<SubJob> subJob) {
        flow.put(subJobName, subJob);
    }
    
}
