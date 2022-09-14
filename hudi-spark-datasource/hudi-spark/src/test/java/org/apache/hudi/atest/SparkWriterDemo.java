package org.apache.hudi.atest;

import com.google.gson.JsonObject;
import org.apache.avro.Schema;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDD$;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.sources.In;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SparkWriterDemo {
  static String tableName = "tb3";
  static String tablePath = ""
      + "file:///Users/wuwenchi/github/hudi/warehouse/database/"
      + tableName;
  static String databasePath = "file:///Users/wuwenchi/github/hudi/warehouse/database/";
  static String pkField = "pk1";
  //  static String parField = "ts1";
  static String parField = "par1";
  static String preCom = "flong1";
  static int parall = 1;
  static SparkSession spark;

  static {
    createSpark();
//    sqlCreate();
  }

  public static void createSpark() {

    spark = SparkSession.builder()
        .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        .appName("spark_to_hudi")
        .master("local[1]")
        .getOrCreate();
  }

  @Test
  public void sqlCreate() {
    spark.sql("" +
        "create table " + tableName + " (\n" +

        "pk1 string, \n " +
        "pk2 string, \n " +

        "par1 string, \n " +
        "par2 string, \n " +

        "fint1 int, \n " +
        "flong1 long \n " +

        ") using hudi \n " +
        "partitioned by (" + parField + ") \n"+
        " tblproperties ( \n " +
//        "hoodie.database.name = 'databaseName', " +
        "hoodie.table.name = '" + tableName + "', \n" +
        "primaryKey = '" + pkField + "', \n" +
        "preCombineField = '" + preCom + "', \n" +
        "hoodie.datasource.write.operation = 'upsert', \n" +
        "hoodie.datasource.write.keygenerator.class = '"+ ComplexAvroKeyGenerator.class.getName() + "' \n" +
        ")\n");
//        ")\n" +
//        "location '" + tablePath + "'");
  }

  @Test
  public void sqlRead() {
    spark.sql("select * from " + tableName).show();
  }

  @Test
  public void sqlInsert() {
    spark.sql("insert into " + tableName + " values ('id9','id9','c','c', 1,1)");
  }

  @Test
  public void testApiWrite() {

    System.out.println(spark.sqlContext().sparkContext().getConf());

    JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
    List<DataSchema> listList = Arrays.asList(
//        DataSchema.getID1_ID2_ParA(),
        DataSchema.getIDx_IDx_ParX_ParA(0, 6, "c")
    );
    JavaRDD<DataSchema> parallelize = jssc.parallelize(listList, 1);
    Dataset<Row> dataFrame = spark.createDataFrame(parallelize, DataSchema.class);


    System.out.println("===========================================");
    System.out.println(dataFrame.collectAsList());
    dataFrame.show();
    System.out.println(dataFrame.schema());

    dataFrame.write()
        .format("hudi")
        .option(HoodieWriteConfig.TBL_NAME.key(), tableName)

        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), pkField)
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), preCom)
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), parField)

        .option(DataSourceWriteOptions.PAYLOAD_CLASS_NAME().key(), "org.apache.hudi.common.model.EventTimeAvroPayload")
        .option(DataSourceWriteOptions.TABLE_TYPE().key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL())
//        .option(DataSourceWriteOptions.TABLE_TYPE().key(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL())
        .option("hoodie.upsert.shuffle.parallelism", parall)
        .option("hoodie.insert.shuffle.parallelism", parall)
        .option(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL())

        .option(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(), HoodieFailedWritesCleaningPolicy.NEVER.name())

//        .option("hoodie.datasource.write.operation", WriteOperationType.UPSERT.name())

        .option(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), ComplexAvroKeyGenerator.class.getName())
        .mode(SaveMode.Append)
        .save(tablePath);
  }

  public static class DataSchema {
    String pk1;
    String pk2;

    String par1;
    String par2;

    int fint1;
    long flong1;

//    Instant ts1;
//    Instant ts2;

    public String getPk1() {
      return pk1;
    }

    public String getPk2() {
      return pk2;
    }

    public String getPar1() {
      return par1;
    }

    public String getPar2() {
      return par2;
    }

    public int getFint1() {
      return fint1;
    }

    public long getFlong1() {
      return flong1;
    }

//    public Instant getTs1() {
//      return ts1;
//    }
//
//    public Instant getTs2() {
//      return ts2;
//    }


    public DataSchema(String pk1, String pk2, String par1, String par2) {
      this.pk1 = pk1;
      this.pk2 = pk2;
      this.par1 = par1;
      this.par2 = par2;
      this.fint1 = 1111;
      this.flong1 = 99;
    }

    public static DataSchema getID1_ID2_ParA() {
      return new DataSchema("id1", "id2", "a", "b");
    }
    public static DataSchema getID2_ID2_ParA() {
      return new DataSchema("id2", "id2", "a", "b");
    }

    public static DataSchema getID2_ID2_ParC() {
      return new DataSchema("id2", "id2", "c", "b");
    }
    public static DataSchema getIDx_ID2_ParX_ParA(int id, String par) {
      return new DataSchema("id"+id, "id2", par, "a");
    }
    public static DataSchema getIDx_IDx_ParX_ParA(int id, int id2, String par) {
      return new DataSchema("id"+id, "id"+id2, par, "a");
    }

    @Override
    public String toString() {
      return "{" +
          "pk1:\"" + pk1 + "\"" +
          ", pk2:\"" + pk2 + '\"' +
          ", par1:\"" + par1 + '\"' +
          ", par2:\"" + par2 + '\"' +
          '}';
    }
  }
}
