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

package my.little.pony.sparkle.impl.spi;

import java.io.IOException;
import java.util.Set;

import my.little.pony.sparkle.impl.spi.util.annotation.AnnotationScanner;
import my.little.pony.sparkle.job.JobContext;
import my.little.pony.sparkle.listener.JobExecutionListener;
import my.little.pony.sparkle.listener.SparkleListener;
import my.little.pony.sparkle.spi.Register;
import org.apache.log4j.Logger;

public class SparkleListenerRegister implements Register {

    private static final Logger LOGGER = Logger.getLogger(SparkleListenerRegister.class);
    
    @Override
    public void register(JobContext jobContext) {
        AnnotationScanner annotationScanner = new AnnotationScanner(jobContext.getJobClass());
        try {

            Set<Class<?>> classes = annotationScanner.scan(SparkleListener.class, jobContext);

            for(Class<?> clasz : classes) {
                jobContext.addListener((JobExecutionListener) clasz.newInstance());
            }

        } catch (IOException | IllegalAccessException | InstantiationException e) {
            LOGGER.error("Error while registering sparkle listener ", e);
        }
    }
}
