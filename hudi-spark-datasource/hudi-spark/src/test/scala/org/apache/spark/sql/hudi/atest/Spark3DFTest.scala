package org.apache.spark.sql.hudi.atest

import org.apache.spark.sql.SaveMode
import org.junit.Test

class Spark3DFTest extends Spark3Test {

  @Test
  def apiWrite(): Unit = {
    val dataFrame = getDataFrame(inputData)

    dataFrame.write.format("hudi")
      //      .bucketBy(10, "par1")
      .options(hudiConfMap)
      .mode(SaveMode.Overwrite)
      .save(getTablePath)
  }


}
