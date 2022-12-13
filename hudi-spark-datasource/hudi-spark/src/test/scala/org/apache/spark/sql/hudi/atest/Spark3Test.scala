package org.apache.spark.sql.hudi.atest

import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieLayoutConfig, HoodieStorageConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex.IndexType.{BUCKET, SIMPLE}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy
import org.apache.hudi.table.storage.HoodieStorageLayout
import org.apache.hudi.{DataSourceWriteOptions, HoodieSparkUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, RowFactory, SaveMode, SparkSession}
import org.junit.Test

import scala.collection.{Seq, mutable}
import scala.collection.mutable.ListBuffer

class Spark3Test {

  var tableName = "tb4"
  val database = "database"
  val databasePath: String = "file:///home/wuwenchi/github/hudi/warehouse/database/"
  val pkField = "pk1,pk2"
  //  private val parField = "par1,par2"
  val preCom = "pre1"
  val parall = 1
  var parNum = 1
  var pkNum = 2
  val parField: String = {
    if (parNum == 1) {
      "par1"
    } else if (parNum == 2) {
      "par1,par2"
    } else {
      ""
    }
  }

  protected lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("hoodie sql test")
    .withExtensions(new HoodieSparkSessionExtension)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("hoodie.insert.shuffle.parallelism", parall)
    .config("hoodie.upsert.shuffle.parallelism", parall)
    .config("hoodie.delete.shuffle.parallelism", parall)
    .config("spark.sql.warehouse.dir", databasePath)
    .config("spark.sql.session.timeZone", "UTC")
    .config(sparkConf())
    .getOrCreate()

  def inputData: Seq[(Int, Int, String, String, Int, Long)] = {
    val tuples = Seq(
//      IDx_IDx_ParX_ParX(1, 1, "a", "b"),
//      IDx_IDx_ParX_ParX(2, 1, "a", "c"),
      IDx_IDx_ParX_ParX(1, 1, "a", "spark"),
      IDx_IDx_ParX_ParX(1, 2, "a", "spark")
    )
    tuples
  }

  def getTablePath: String = {
    databasePath + tableName
  }

  def hudiConfMap: mutable.HashMap[String, String] = {
    val conf = new mutable.HashMap[String, String]

    conf += (HoodieWriteConfig.TBL_NAME.key -> tableName)

    val tableType = DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL
    conf += (DataSourceWriteOptions.TABLE_TYPE.key() -> tableType)
    conf += ("type" -> tableType)
    //    conf += (DataSourceWriteOptions.TABLE_TYPE.key() -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL())
    conf += (DataSourceWriteOptions.OPERATION.key() -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
//    conf += (DataSourceWriteOptions.OPERATION.key() -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)

//    conf += (DataSourceWriteOptions.RECORDKEY_FIELD.key() -> pkField)
//    conf += (DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> preCom)

    // key gen
    if (pkNum == 2) {
      conf += ("primaryKey" -> "pk1,pk2")
    } else {
      conf += ("primaryKey" -> "pk1")
    }
    //    conf += (HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key() -> "org.apache.hudi.keygen.ComplexAvroKeyGenerator")
    //    conf += (HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key() -> "org.apache.hudi.keygen.ComplexKeyGenerator")
    //    conf += (HoodieWriteConfig.KEYGENERATOR_TYPE.key() -> "complex")

    // partition
    conf += (DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> parField)
    conf += (KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key -> false.toString)

    conf += (DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key() -> "org.apache.hudi.common.model.EventTimeAvroPayload")

    // metadata table enable
    conf += (HoodieMetadataConfig.ENABLE.key() -> "false")

    // inline compaction
    conf += (HoodieCompactionConfig.INLINE_COMPACT.key() -> "true")
    conf += (HoodieCompactionConfig.INLINE_LOG_COMPACT.key() -> "true")
    conf += (HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "100")
    conf += (HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key() -> CompactionTriggerStrategy.NUM_COMMITS.name)

    // simple index
//    conf += (HoodieIndexConfig.INDEX_TYPE.key() -> SIMPLE.name())

//    // bucket index
    conf += (HoodieIndexConfig.INDEX_TYPE.key() -> BUCKET.name())
    conf += (HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key() -> "32")
    conf += (HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key() -> "pk1")
    conf += (HoodieIndexConfig.BUCKET_INDEX_MIN_NUM_BUCKETS.key() -> "32")
    conf += (HoodieIndexConfig.BUCKET_INDEX_MAX_NUM_BUCKETS.key() -> "256")
    conf += (HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE.key() -> "CONSISTENT_HASHING")
    conf += (HoodieIndexConfig.BUCKET_SPLIT_THRESHOLD.key() -> "1.1")
    conf += (HoodieIndexConfig.BUCKET_MERGE_THRESHOLD.key() -> "0.9")
//    conf += (HoodieLayoutConfig.LAYOUT_TYPE.key() -> HoodieStorageLayout.LayoutType.BUCKET.name())
//    conf += (HoodieLayoutConfig.LAYOUT_PARTITIONER_CLASS_NAME.key() -> classOf[SparkBucketIndexPartitioner[_]].getName)

    conf += ("hoodie.upsert.shuffle.parallelism" -> parall.toString)
    conf += ("hoodie.insert.shuffle.parallelism" -> parall.toString)

    // 不删除之前写失败的commit
    //    conf += (HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key() -> HoodieFailedWritesCleaningPolicy.NEVER.name())

    conf
  }

  def sparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
    if (HoodieSparkUtils.gteqSpark3_2) {
      sparkConf.set("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    }
    sparkConf
  }

  @Test
  def testHudiConf(): Unit = {
    val conf = hudiConfMap
    val str = conf.map(kv => kv._1 + "='" + kv._2 + "'").mkString(",")
    println(str)
  }



  def IDx_IDx_ParX_ParA(id1: Int, id2: Int, par1: String): (String, String, String, String, Int, Long) = {
    ("id" + id1, "id" + id2, par1, "a", 12, 4)
  }

  def IDx_IDx_ParX_ParX(id1: Int, id2: Int, par1: String, par2: String): (Int, Int, String, String, Int, Long) = {
    (id1, id2, par1, par2, 12, 28)
  }



  def getDataFrame(inputData : Seq[(Int, Int, String, String, Int, Long)]): DataFrame = {
    import spark.implicits._

    inputData.toDF("pk1", "pk2", "par1", "par2", "fint1", "pre1")

//    Seq(
//      IDx_IDx_ParX_ParA(1, 31, "b"),
//      IDx_IDx_ParX_ParA(1, 31, "a"),
//      IDx_IDx_ParX_ParA(9, 2, "a")
//    )
//      .toDF("pk1", "pk2", "par1", "par2", "fint1", "pre1")
  }

  def getInputString(inputData: Seq[(Int, Int, String, String, Int, Long)]): String = {
    getInputString(inputData, parNum)
  }

  def getInputString(inputData : Seq[(Int, Int, String, String, Int, Long)], par:Int): String = {
    if (par == 1) {
      inputData.map(data => {
        s"""
           |(${data._1}, ${data._2}, "${data._4}", ${data._5}, ${data._6}, "${data._3}") """.stripMargin
      }).mkString(", ")
    } else if (par == 2) {
      inputData.map(data => {
        s"""
           |(${data._1}, ${data._2}, ${data._5}, ${data._6}, "${data._3}", "${data._4}") """.stripMargin
      }).mkString(", ")
    } else {
      inputData.map(data => {
        s"""
           |(${data._1}, ${data._2}, "${data._3}", "${data._4}", ${data._5}, ${data._6}) """.stripMargin
      }).mkString(", ")
    }
  }

  def getInputString: String = {
    inputData.map(data => {
      s"""
         |("${data._1}", "${data._2}", "${data._3}", "${data._4}", ${data._5}, ${data._6}) """.stripMargin
    }).mkString(", ")
  }

}