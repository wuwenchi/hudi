package org.apache.spark.sql.hudi.atest

import org.junit.Test

class Spark3SqlTest extends Spark3Test {

  def sqlCreate(): Unit = {
    sqlCreate(parNum)
  }

  def sqlCreate(par:Int): Unit = {

    var partition = ""
    if (par == 1) {
      partition = " partitioned by (par1) "
    } else if (par == 2) {
      partition = " partitioned by (par1, par2) "
    }

    spark.sql(
      s"""
         |CREATE TABLE $tableName (
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
         | tblproperties ($hudiConfString) $partition
         | location '$getTablePath'
       """.stripMargin)
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
    val sqlString = s"insert into $tableName values ${getInputString(inputData)}"
    spark.sql(sqlString).show(false)
  }

  @Test
  def sqlRead(): Unit = {
    spark.sql(s"select pk1,pk2,par1,par2,_hoodie_file_name from $tableName").show(false)
  }

  @Test
  def testSqlInsert(): Unit = {
    tableName = "tb6"
    sqlCreate()
    sqlInsertInto()
    sqlRead()

  }

}
