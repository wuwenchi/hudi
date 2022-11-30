package org.apache.spark.sql.hudi.atest

import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.config.{HoodieIndexConfig, HoodieLayoutConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex.IndexType.{BUCKET, SIMPLE}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner
import org.apache.hudi.table.storage.HoodieStorageLayout
import org.apache.hudi.{DataSourceWriteOptions, HoodieSparkUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, RowFactory, SaveMode, SparkSession}
import org.junit.Test

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Spark2Test {

  private val tableName = "tb4"
  private val database = "" + "file:///Users/wuwenchi/github/hudi/warehouse/database/"
  private val tablePath = database + tableName
  private val pkField = "pk1"
  private val parField = "par1"
  //  private val parField = "par1,par2"
  private val preCom = "flong1"
  private val parall = 1

  protected lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("hoodie sql test")
    .withExtensions(new HoodieSparkSessionExtension)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("hoodie.insert.shuffle.parallelism", parall)
    .config("hoodie.upsert.shuffle.parallelism", parall)
    .config("hoodie.delete.shuffle.parallelism", parall)
    .config("spark.sql.warehouse.dir", database)
    .config("spark.sql.session.timeZone", "UTC")
    .config(new SparkConf())
    .getOrCreate()

  def sparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
    if (HoodieSparkUtils.gteqSpark3_2) {
      sparkConf.set("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    }
    sparkConf
  }

  @Test
  def sqlInsert(): Unit = {

    sqlCreate()

//        spark.sql(s"insert into $tableName partition(par1 = 'a') values ('id4','id2','a',1,2), ('id1','id2','a',1,2)")
    //    spark.sql(s"insert overwrite table $tableName values ('id4','id2','a','a',1,2), ('id1','id2','a','a',1,2)")
    //    spark.sql(s"insert overwrite $tableName      partition(par1 = 'a') select 'id4','id2','a',1,2")
    //    spark.sql(s"insert overwrite $tableName      partition(par1 = 'a') select 'id4','id2','a',1,2")
    //    spark.sql(s"insert overwrite table tb5 partition(par1 = 'a') select 'id4','id2','a',1,2")
//    spark.sql(s"insert overwrite $tableName partition(par1 = 'a') values ('id4','id3','a',1,2), ('id1','id3','a',1,2)")
    //    sqlRead()
    spark.sql(s"select _hoodie_file_name,pk1,pk2 from $tableName").show(false)
    //    spark.sql(s"select _hoodie_file_name,pk1,pk2 from $tableName timestamp as of '20221019181616510'").show(false)
    //    spark.sql(s"select * from $tableName").show()
  }

  def sqlCreate(): Unit = {
    spark.sql(
      s"""
         |create table $tableName (
         |
         |  pk1 string,
         |  pk2 string,
         |
         |  par1 string,
         |  par2 string,
         |
         |  fint1 int,
         |  flong1 long
         |
         |) using hudi
         | tblproperties ($hudiConfString)
         | location '$tablePath'
       """.stripMargin)
  }

  def hudiConfMap: mutable.HashMap[String, String] = {
    val conf = new mutable.HashMap[String, String]

    conf += (HoodieWriteConfig.TBL_NAME.key -> tableName)

    val tableType = DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL
    conf += (DataSourceWriteOptions.TABLE_TYPE.key() -> tableType)
    conf += ("type" -> tableType)
    //    conf += (DataSourceWriteOptions.TABLE_TYPE.key() -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL())
    conf += (DataSourceWriteOptions.OPERATION.key() -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
    conf += (DataSourceWriteOptions.OPERATION.key() -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)

    conf += (DataSourceWriteOptions.RECORDKEY_FIELD.key() -> pkField)
    conf += ("primaryKey" -> pkField)
    conf += (DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> parField)
    conf += (DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> preCom)

    // key gen
    //    conf += (HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key() -> "org.apache.hudi.keygen.ComplexAvroKeyGenerator")
    //    conf += (HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key() -> "org.apache.hudi.keygen.ComplexKeyGenerator")
    //    conf += (HoodieWriteConfig.KEYGENERATOR_TYPE.key() -> "complex")
    conf += (KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key -> false.toString)

    conf += (DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key() -> "org.apache.hudi.common.model.EventTimeAvroPayload")

    // metadata table enable
    conf += (HoodieMetadataConfig.ENABLE.key() -> "false")

    // simple index
    conf += (HoodieIndexConfig.INDEX_TYPE.key() -> SIMPLE.name())

//    // bucket index
//    conf += (HoodieIndexConfig.INDEX_TYPE.key() -> BUCKET.name())
//    conf += (HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key() -> "2")
//    conf += (HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key() -> "pk1")
//    conf += (HoodieLayoutConfig.LAYOUT_TYPE.key() -> HoodieStorageLayout.LayoutType.BUCKET.name())
//    conf += (HoodieLayoutConfig.LAYOUT_PARTITIONER_CLASS_NAME.key() -> classOf[SparkBucketIndexPartitioner[_]].getName)

    conf += ("hoodie.upsert.shuffle.parallelism" -> parall.toString)
    conf += ("hoodie.insert.shuffle.parallelism" -> parall.toString)

    // 不删除之前写失败的commit
    //    conf += (HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key() -> HoodieFailedWritesCleaningPolicy.NEVER.name())

    conf
  }

  def hudiConfString: String = {
    hudiConfMap.map(kv => kv._1 + "='" + kv._2 + "'").mkString(",")
  }

  @Test
  def testHudiConf(): Unit = {
    println(hudiConfMap)
    val conf = hudiConfMap
    val str = conf.map(kv => kv._1 + "='" + kv._2 + "'").mkString(",")
    println(str)
  }

  @Test
  def sqlRead(): Unit = {
    sqlCreate()
    spark.sql(s"select _hoodie_file_name,pk1,pk2 from $tableName").show(false)
  }

  def IDx_IDx_ParX_ParA(id1: Int, id2: Int, par1: String): (String, String, String, String, Int, Long) = {
    ("id" + id1, "id" + id2, par1, "a", 12, 4)
  }

  def getDataFrame1: DataFrame = {
    val seq = Seq(
      DataSchema.getIDx_ID2_ParX_ParA("id7", "a")
    )
    val rdd = spark.sparkContext.parallelize(seq, 1).map { item =>
      val c1 = Integer.valueOf(item.fint1)
      val c2 = java.lang.Long.valueOf(item.fint1)
      RowFactory.create(item.pk1, item.pk2, item.par1, item.par2, c1, c2)
    }
    val structType = new StructType()
      .add("pk1", "string")
      .add("pk2", "string")
      .add("par1", "string")
      .add("par2", "string")
      .add("fint1", "int")
      .add("flong1", "long")

    spark.createDataFrame(rdd, structType)
  }

  def inputData:  Seq[(String, String, String, String, Int, Long)] = {
    val tuples = Seq(
      IDx_IDx_ParX_ParA(1, 31, "b"),
      IDx_IDx_ParX_ParA(1, 31, "a"),
      IDx_IDx_ParX_ParA(9, 2, "a")
    )
    tuples
  }

  def getDataFrame2: DataFrame = {
    import spark.implicits._

    Seq(
      IDx_IDx_ParX_ParA(1, 31, "b"),
      IDx_IDx_ParX_ParA(1, 31, "a"),
      IDx_IDx_ParX_ParA(9, 2, "a")
    )
      .toDF("pk1", "pk2", "par1", "par2", "fint1", "flong1")
  }

  def getInputString: String = {
    inputData.map(data => {
      s"""
         |("${data._1}", "${data._2}", "${data._3}", "${data._4}", ${data._5}, ${data._6}) """.stripMargin
    }).mkString(", ")
  }

  @Test
  def sqlWrite(): Unit = {
    sqlCreate
//    spark.sql(s"use $databaseName")
//    spark.sql(s"insert into $tableName values $getInputString")
    spark.sql(s"select * from $tableName").show()
  }


  @Test
  def apiWrite(): Unit = {
    //    val dataFrame = getDataFrame1
    val dataFrame = getDataFrame2

    dataFrame.write.format("hudi")
      //      .bucketBy(10, "par1")
      .options(hudiConfMap)
      .mode(SaveMode.Overwrite)
      .save(tablePath)
  }


  object DataSchema { //    Instant ts1;
    //    Instant ts2;
    //    public Instant getTs1() {
    //      return ts1;
    //    }
    //
    //    public Instant getTs2() {
    //      return ts2;
    def getID1_ID2_ParA = new DataSchema("id1", "id2", "a", "b", 1, 1)

    def getID2_ID2_ParA = new DataSchema("id2", "id2", "a", "b", 1, 1)

    def getID2_ID2_ParC = new DataSchema("id2", "id2", "c", "b", 1, 1)

    def getIDx_ID2_ParX_ParA(pk1: String, par1: String) = new DataSchema(pk1, "id2", par1, "b", 1, 1)
  }

  //  class DataSchema(var pk1: Int, var par1: String) {
  //
  //  }

  class DataSchema(var pk1: String, var pk2: String, var par1: String, var par2: String, var fint1: Int, var flong1: Long) {
    //    private val fint1 = 0
    //    private val flong1 = 0L

    def getPk1: String = pk1

    def getPk2: String = pk2

    def getPar1: String = par1

    def getPar2: String = par2

    def getFint1: Int = fint1

    def getFlong1: Long = flong1
  }

}