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

package my.little.pony.sparkle;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;

import my.little.pony.sparkle.io.SparkleResources;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class Sparkle {

    private final static Logger LOGGER = LogManager.getLogger(Sparkle.class);

    static {
        if (isSubmitted() || runOnCluster()) {
            // nothing  for now
        } else {
            installWinutils();

            SparkSession sparkSession;
            if (!Boolean.getBoolean("sparkle.standalone.session.disable")) {
                sparkSession = SparkSession.builder().master("local").appName("standalone").getOrCreate();
                SparkSession.setActiveSession(sparkSession);
                SparkSession.setDefaultSession(sparkSession);
            }
        }
    }

    public static SparkSession spark() {
        return SparkSession.getActiveSession().get();
    }

    public static SparkContext sc() {
        return spark().sparkContext();
    }

    public static void description(String description) {
        sc().setJobDescription(description);
    }

    public static void description(String groupId, String description) {
        sc().setJobGroup(groupId, description, false);
    }


    private static boolean isSubmitted() {
        return Boolean.getBoolean("SPARK_SUBMIT");
    }

    private static boolean runOnCluster() {
        String sparkMaster = System.getProperty("spark.master");
        return StringUtils.isNotBlank(sparkMaster) && !sparkMaster.startsWith("local") ;
    }


    private static void installWinutils() {
        if (SystemUtils.IS_OS_WINDOWS &&
           (StringUtils.isBlank(System.getenv("HADOOP_HOME")) && StringUtils.isBlank(System.getProperty("hadoop.home.dir")))) {

            File hadoopDir = new File(SystemUtils.getJavaIoTmpDir(), "hadoop");
            File binDir = new File(hadoopDir, "bin");

            if (!binDir.exists() && !binDir.mkdirs()) {
               throw new RuntimeException("Could not install winutils.exe");
            }


            try {

                for(String file : new String[] {"winutils.exe", "hadoop.dll"}) {
                    URL url = SparkleResources.getResource("hadoop/bin/" + file);

                    if (url != null) {
                        IOUtils.copy(url.openStream(), new FileOutputStream(new File(binDir, file)));
                    }

                }

                System.setProperty("hadoop.home.dir", hadoopDir.getAbsolutePath());
                LOGGER.info("Setting 'hadoop.home.dir' to " + System.getProperty("hadoop.home.dir"));
            } catch (IOException e) {
                throw new RuntimeException("Could not install wintutils.exe", e);
            }

        }

    }


}
