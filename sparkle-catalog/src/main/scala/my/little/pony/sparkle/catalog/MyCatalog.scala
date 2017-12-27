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

package my.little.pony.sparkle.catalog

import java.net.URI

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class MyCatalog(externalCatalog: ExternalCatalog) extends ExternalCatalog {
  override protected def doCreateDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = ???

  override protected def doDropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = ???

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = ???

  override def getDatabase(db: String): CatalogDatabase = ???

  override def databaseExists(db: String): Boolean = {
    println("database exists : " + db)
    if (externalCatalog.databaseExists(db)) true else db == "titanic"
  }

  override def listDatabases(): Seq[String] = ???

  override def listDatabases(pattern: String): Seq[String] = ???

  override def setCurrentDatabase(db: String): Unit = ???

  override protected def doCreateTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = ???

  override protected def doDropTable(db: String, table: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = ???

  override protected def doRenameTable(db: String, oldName: String, newName: String): Unit = ???

  override def alterTable(tableDefinition: CatalogTable): Unit = ???

  override def alterTableDataSchema(db: String, table: String, schema: StructType): Unit = ???

  override def getTable(db: String, table: String): CatalogTable = {
    println("getTable : (" +db +"," + table + ")")

    val tableIdentifier = TableIdentifier(table, Option(db))
    val catalogTableType = CatalogTableType.EXTERNAL

    val catalogStorageFormat = CatalogStorageFormat(
      Option(URI.create("/Users/stephanetrou/projets/titanic/train.csv")),
      Option("csv"),
      Option.empty,
      Option.empty,
      false,
      Map("header" -> "true", "separator" -> ",")
    )

    CatalogTable(
      tableIdentifier,
      catalogTableType,
      catalogStorageFormat,
      StructType(
        Seq(
          StructField("survived", DataTypes.IntegerType),
          StructField("pclass", DataTypes.IntegerType),
          StructField("name", DataTypes.StringType),
          StructField("sex", DataTypes.StringType),
          StructField("age", DataTypes.IntegerType),
          StructField("sibsp", DataTypes.IntegerType),
          StructField("parch", DataTypes.IntegerType),
          StructField("ticket", DataTypes.StringType),
          StructField("fare", DataTypes.DoubleType),
          StructField("cabin", DataTypes.StringType),
          StructField("embarke", DataTypes.StringType)
        )
      ),
      Option("csv")
    )
  }
  
  override def tableExists(db: String, table: String): Boolean = {
      db == "titanic" && table == "train"
  }

  override def listTables(db: String): Seq[String] = ???

  override def listTables(db: String, pattern: String): Seq[String] = ???

  override def loadTable(db: String, table: String, loadPath: String, isOverwrite: Boolean, isSrcLocal: Boolean): Unit = ???

  override def loadPartition(db: String, table: String, loadPath: String, partition: TablePartitionSpec, isOverwrite: Boolean, inheritTableSpecs: Boolean, isSrcLocal: Boolean): Unit = ???

  override def loadDynamicPartitions(db: String, table: String, loadPath: String, partition: TablePartitionSpec, replace: Boolean, numDP: Int): Unit = ???

  override def createPartitions(db: String, table: String, parts: Seq[CatalogTablePartition], ignoreIfExists: Boolean): Unit = ???

  override def dropPartitions(db: String, table: String, parts: Seq[TablePartitionSpec], ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit = ???

  override def renamePartitions(db: String, table: String, specs: Seq[TablePartitionSpec], newSpecs: Seq[TablePartitionSpec]): Unit = ???

  override def alterPartitions(db: String, table: String, parts: Seq[CatalogTablePartition]): Unit = ???

  override def getPartition(db: String, table: String, spec: TablePartitionSpec): CatalogTablePartition = ???

  override def getPartitionOption(db: String, table: String, spec: TablePartitionSpec): Option[CatalogTablePartition] = ???

  override def listPartitionNames(db: String, table: String, partialSpec: Option[TablePartitionSpec]): Seq[String] = ???

  override def listPartitions(db: String, table: String, partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = ???

  override def listPartitionsByFilter(db: String, table: String, predicates: Seq[Expression], defaultTimeZoneId: String): Seq[CatalogTablePartition] = ???

  override protected def doCreateFunction(db: String, funcDefinition: CatalogFunction): Unit = ???

  override protected def doDropFunction(db: String, funcName: String): Unit = ???

  override protected def doRenameFunction(db: String, oldName: String, newName: String): Unit = ???

  override def getFunction(db: String, funcName: String): CatalogFunction = externalCatalog.getFunction(db, funcName)

  override def functionExists(db: String, funcName: String): Boolean = externalCatalog.functionExists(db, funcName)

  override def listFunctions(db: String, pattern: String): Seq[String] = externalCatalog.listFunctions(db, pattern)
}


