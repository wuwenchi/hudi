package org.apache.spark.sql.hudi.atest

import org.apache.spark.sql.DataFrame
import org.junit.Test

/*

bin/spark-sql --packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.12.1 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog'

bucket index：

CREATE TABLE IF NOT EXISTS tb4 (
  id int ,
  pre string,
  price double,
  par string
) using hudi
 partitioned by (par)
 options (
  type='mor',
  primaryKey='id',
  preCombineField='pre',
  hoodie.bucket.index.num.buckets='2',
  hoodie.index.type='BUCKET',
  hoodie.bucket.index.hash.field='id',
  hoodie.datasource.write.hive_style_partitioning='false',
  hoodie.datasource.write.operation='upsert'
);
 location 'file:///tmp/hudi/database/tb4'

 */

class Spark3SqlTest extends Spark3Test {

  @Test
  def sqlCreate(): Unit = {

    // 默认两个主键，一个分区
    val ddl = getSqlCreateDDL
//    println(ddl)
    spark.sql(ddl)
  }

  def getSqlCreateDDL: String = {

    var partition = ""
    if (parNum == 1) {
      partition = " partitioned by (par1) "
    } else if (parNum == 2) {
      partition = " partitioned by (par1, par2) "
    }

    val DDL =
      s"""
         |CREATE TABLE IF NOT EXISTS $tableName (
         |
         |  pk1 int,
         |  pk2 int,
         |
         |  par1 string,
         |  par2 string,
         |
         |  fint1 int,
         |  pre1 long
         |
         |) using hudi
         | $partition
         | location '$getTablePath'
         | options (
         | ${hudiConfMap.map(kv => "  " + kv._1 + " = '" + kv._2 + "'").mkString(",\n")}
         | );
     """.stripMargin

    return DDL
  }

  @Test
  def sqlInsertOverwrite(): Unit = {


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

  @Test
  def sqlInsertInto(): Unit = {
    var sqlString = s"insert into $tableName values ${getInputString(inputData)}"
//    sqlString =
//      s"""
//         |insert into $tableName values
//         |(1,1,'a','spark',1,2),
//         |(20,1,'a','spark',1,2),
//         |(1,1,'b','spark',1,2),
//         |(20,1,'b','spark',1,2)
//         |""".stripMargin
//        print(sqlString)
    spark.sql(sqlString).show(false)
  }

  def inputSome: Seq[(Int, Int, String, String, Int, Long)] = {
    val builder = Seq.newBuilder[(Int, Int, String, String, Int, Long)]
    var a = 0;
    // for 循环
    for (a <- 101 to 200) {
      builder += IDx_IDx_ParX_ParX(1, a, "a", "spark")
    }
    return builder.result()
    val tuples = Seq(
      //      IDx_IDx_ParX_ParX(1, 1, "a", "b"),
      //      IDx_IDx_ParX_ParX(2, 1, "a", "c"),
      IDx_IDx_ParX_ParX(1, 18, "a", "spark"),
      IDx_IDx_ParX_ParX(1, 19, "a", "spark")
    )
    tuples
  }

  @Test
  def sqlRead(): Unit = {
//    spark.sql(s"select pk1,pk2,par1,par2,_hoodie_file_name from $tableName where pk2 > 250").show(false)
    spark.sql(s"select pk1,pk2,par2 from $tableName").show(false)
  }

  @Test
  def testSqlInsert(): Unit = {
    tableName = "tb5"
    sqlCreate()
    sqlInsertInto()
    sqlRead()

  }

  @Test
  def sqlClusteringByTable(): Unit = {
    tableName = "tb5"
    sqlCreate()
    spark.sql(s"call run_clustering(table => '$tableName')")
  }

  @Test
  def sqlClusteringByPath(): Unit = {
    tableName = "tb5"
//    sqlCreate()
    spark.sql(s"call run_clustering(path => '$getTablePath')")
//    spark.sql(s"call run_clustering(path => /users/wuwenchi/github/hudi/warehouse/database/tb5)")
//    sqlClustering()
  }

}