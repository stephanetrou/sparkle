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

package my.little.pony.sparkle.sample.job.interceptors;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;
import com.google.common.reflect.ClassPath;
import my.little.pony.sparkle.listener.JobExecutionListener;
import my.little.pony.sparkle.listener.SparkleListener;
import my.little.pony.sparkle.listener.event.JobStart;
import org.apache.log4j.Logger;

@SparkleListener
public class MavenModulesInfo implements JobExecutionListener {

    private final static Logger LOGGER = Logger.getLogger(MavenModulesInfo.class);

    private String filter;

    public MavenModulesInfo() {
        this.filter = "org.apache.spark";
    }

    @Override
    public void jobStarted(JobStart jobStart) {
        try {

            List<String> infos = ClassPath.from(jobStart.getJobContext().getJobClass().getClassLoader()).getResources()
                    .stream()
                    .filter(filterResources())
                    .map(loadProperties())
                    .map(p -> String.format("* %s (%s)", p.getProperty("artifactId"), p.getProperty("version")))
                    .collect(toList());

            if (!infos.isEmpty()) {
                LOGGER.info(String.format("Found %s artifacts for groupId %s", infos.size(), filter));
                infos.forEach(LOGGER::info);
            } else {
                LOGGER.info("Don't find artifact of groupId " + filter);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Predicate<ClassPath.ResourceInfo> filterResources() {
        return r -> r.getResourceName().startsWith("META-INF/maven/" + filter) && r.getResourceName().endsWith("pom.properties");
    }

    private Function<ClassPath.ResourceInfo, Properties> loadProperties() {
        return r -> {
            Properties p = new Properties();
            try {
                p.load(r.url().openStream());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return p;
        };
    }


}
