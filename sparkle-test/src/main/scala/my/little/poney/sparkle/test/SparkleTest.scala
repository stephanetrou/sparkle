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

package my.little.poney.sparkle.test

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.junit.{Before, ComparisonFailure}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}



class SparkleTest extends JavaDatasetSuiteBase {

  System.setProperty("sparkle.standalone.session.disable", "true")

  override def runBefore(): Unit = {
    super.runBefore()
    SparkSession.setActiveSession(spark)
  }

  override def sqlBeforeAllTestCases(): Unit = {
    super.sqlBeforeAllTestCases()
    SparkSession.setActiveSession(null)
  }
  
  override def assertDataFrameEquals(expected: DataFrame, result: DataFrame): Unit = decodeAssertionError {
    super.assertDataFrameEquals(expected, result)
  }

  override def assertDatasetEquals[U](expected: Dataset[U], result: Dataset[U]): Unit = decodeAssertionError {
    super.assertDatasetEquals(expected, result)
  }

  override def assertDatasetApproximateEquals[U](expected: Dataset[U], result: Dataset[U], tol: Double): Unit = decodeAssertionError {
    super.assertDatasetApproximateEquals(expected, result, tol)
  }

  override def assertDatasetEquals[U](expected: Dataset[U], result: Dataset[U])(implicit UCT: ClassTag[U]): Unit = decodeAssertionError {
    super.assertDatasetEquals(expected, result)
  }

  override def assertDatasetApproximateEquals[U](expected: Dataset[U], result: Dataset[U], tol: Double)(implicit UCT: ClassTag[U]): Unit = decodeAssertionError {
    super.assertDatasetApproximateEquals(expected, result, tol)
  }

  override def assertDataFrameApproximateEquals(expected: DataFrame, result: DataFrame, tol: Double): Unit = decodeAssertionError {
    super.assertDataFrameApproximateEquals(expected, result, tol)
  }

  private def decodeAssertionError(r: => Unit): Unit = {
    Try(r) match {
      case Failure(e:AssertionError) => throw decodeAssertionError(e)
      case Failure(e) => throw e
      case _ => None
    }
  }

  private def decodeAssertionError(e : AssertionError) : Throwable = e.getMessage match {
      case SparkleTest.SCHEMA(expected, actual) => schemaError(expected, actual)
      case SparkleTest.SAMPLES(message) => sampleError(message)
      case _ => e
    }

  private def schemaError(expected : String, actual : String): ComparisonFailure ={
    new ComparisonFailure("Schema Error", expected.replace(" ", "\n"), actual.replace(" ", "\n"))
  }

  private def sampleError(message : String): ComparisonFailure = {
    val f = (a: (Array[String], Array[String])) => (a._1.mkString("\n"), a._2.mkString("\n"))

    val t = f(message.replaceAll("""\]\)\), """, """\]\)\)\]\)\), """).split("""\]\)\), """).map(_ match {
      case SparkleTest.SAMPLE(expected, actual) => (expected, actual)
      case _ => ("", "")
    }).unzip)

    new ComparisonFailure("Not Equal Sample : ", t._1, t._2)
  }


}

object SparkleTest {
  val SCHEMA = """expected:<StructType\((.*)\)> but was:<StructType\((.*)\)>""".r
  val SAMPLES = """Not Equal Sample: (.*)""".r
  val SAMPLE = """\(\d+,\((\[.*\]),(\[.*\])\)\)""".r
}