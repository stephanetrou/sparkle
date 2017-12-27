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

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class Sparkle {
    
    static {
        if (isSubmitted() || runOnCluster()) {
            // nothing  for now
        } else {
            SparkSession sparkSession = SparkSession.builder().master("local").appName("standalone").getOrCreate();
            SparkSession.setActiveSession(sparkSession);
            SparkSession.setDefaultSession(sparkSession);
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
}
