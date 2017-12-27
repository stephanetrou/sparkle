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

package my.little.pony.sparkle.metrics;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.executor.InputMetrics;
import org.apache.spark.executor.OutputMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class MetricsListener extends SparkListener {

    public MetricsListener() {}

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        taskMetrics(taskEnd.taskMetrics());
    }

    private void taskMetrics(TaskMetrics taskMetrics) {
        inputMetrics(taskMetrics.inputMetrics());
        outputMetrics(taskMetrics.outputMetrics());
    }

    private void inputMetrics(InputMetrics inputMetrics) {
        System.out.printf("Input Metrics - records : %s bytes : %s\n", inputMetrics.recordsRead(), inputMetrics.bytesRead());
    }

    private void outputMetrics(OutputMetrics outputMetrics) {
        System.out.printf("Output Metrics - records : %s bytes : %s\n", outputMetrics.recordsWritten(), outputMetrics.bytesWritten());
    }


    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        seq(stageCompleted.stageInfo().rddInfos()).forEach(
                System.out::println
        );
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        super.onOtherEvent(event);

        if (event instanceof SparkListenerSQLExecutionStart) {
            SparkListenerSQLExecutionStart e = ((SparkListenerSQLExecutionStart) event);
            //System.out.println("other event : " + e.description());
            //System.out.println("other phy : " + e.physicalPlanDescription());
            showPlan("", e.sparkPlanInfo());
        } else if (event instanceof SparkListenerSQLExecutionEnd) {
            SparkListenerSQLExecutionEnd e = (SparkListenerSQLExecutionEnd) event;
            System.out.println("sparksql end : " + e);
        } else if (event instanceof SparkListenerDriverAccumUpdates) {
            SparkListenerDriverAccumUpdates e = (SparkListenerDriverAccumUpdates) event;

            System.out.println(e);

        } else {
            System.out.println("Other Event : " + event.getClass());
        }
    }

    public void showPlan(String lpad, SparkPlanInfo info) {
        System.out.println(lpad + info.simpleString() + " (" + info.nodeName() + ") " );

        //seq(info.metrics()).forEach(metric -> System.out.println(String.format(lpad + "-> %s : %s (id:%s)", metric.name(), metric.metricType(), metric.accumulatorId())));
        map(info.metadata()).forEach(
                (k, v) -> System.out.println(lpad + " * " + k + " : " + v)
        );

        seq(info.children()).forEach(child -> showPlan(lpad + "\t", child));
    }
    
    private static <T> List<T> seq(Seq<T> seq) {
        return JavaConversions.seqAsJavaList(seq);
    }

    private static <K, V> Map<K, V> map(scala.collection.Map<K, V> map) {
        return JavaConversions.mapAsJavaMap(map);
    }

    private static <T> Iterator<T> it(scala.collection.Iterator<T> it) {
        return JavaConversions.asJavaIterator(it);
    }

}
