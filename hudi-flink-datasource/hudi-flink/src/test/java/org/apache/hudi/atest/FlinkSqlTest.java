package org.apache.hudi.atest;

import org.junit.Test;

import static org.apache.hudi.utils.TestConfigurations.catalog;

public class FlinkSqlTest extends FlinkTest {

  @Test
  public void sqlCreateCatalog() {

    String catalogName = "hudi_local";
    String hudiCatalogDDL = catalog(catalogName)
        .catalogPath(getWarehousePath())
        .end();

    tEnv.executeSql(hudiCatalogDDL);
    tEnv.executeSql("use catalog " + catalogName);
    tEnv.executeSql("use " + databaseName);
  }

  @Test
  public void sqlCreate() {

    sqlCreateCatalog();

    String ddl = getDDLString();
    tEnv.executeSql(ddl);
  }

  public void sqlCreate(String ddl) {
    String hudiCatalogDDL = catalog("hudi_catalog")
        .catalogPath(getWarehousePath())
        .end();

    tEnv.executeSql(hudiCatalogDDL);
    tEnv.executeSql("use catalog " + ("hudi_catalog"));
    tEnv.executeSql("use " + databaseName);
    tEnv.executeSql(ddl);
  }

  public String getDDLString() {
    return new Sql(tableName)

//        .field("_hoodie_file_name string")

        .field("pk1 int")
        .field("pk2 int")

        .field("par1 string")
        .field("par2 string")

        .field("fint1 int")
        .field("flong1 bigint")

//        .field("ts1 timestamp(3)")
//        .field("ts2 timestamp(3)")

        .options(getHudiConf().toMap())
        .end();
  }

  @Test
  public void sqlInsertInto() {
    String s = "insert into " + tableName + " values \n" + getInputString(getInput());
//    System.out.println(s);
    tEnv.executeSql(s).print();
  }

  @Test
  public void sqlRead() {
    tEnv.executeSql("select * from " + tableName).print();

//    tEnv.executeSql("select * from " + tableName).collect().forEachRemaining(System.out::println);
  }

  @Test
  public void testSqlInsertInto() {
    tableName = "tb1";
    sqlCreate();
    sqlInsertInto();
    sqlRead();
  }


  @Test
  public void sqlCreateWithoutPrimaryKey() {
    sqlCreateCatalog();
    tEnv.executeSql("" +
        "CREATE TABLE tb1(\n" +
        "  id int, \n" +
        "  name string, \n" +
        "  price double,\n" +
        "  par string\n" +
        ") PARTITIONED BY (par)\n" +
        "WITH (\n" +
        "'connector' = 'hudi',\n" +
//        "'table.type' = 'MERGE_ON_READ' \n" +
        "'table.type' = 'COPY_ON_WRITE' \n" +
        ");" +
        "");
    // 两种模式都会报错
  }

  @Test
  public void sqlCreateWeiBiao1() {
    sqlCreateCatalog();
    tEnv.executeSql("" +
        "CREATE TABLE IF NOT EXISTS weibiao1(\n" +
        "  id int PRIMARY KEY NOT enforced, \n" +
        "  price double,\n" +
        "  par string\n" +
        ") WITH (\n" +
        "'connector' = 'hudi',\n" +
        "'table.type' = 'COPY_ON_WRITE',\n" +
        "'precombine.field' = 'no_precombine'\n" +
        ");" +
        "").print();
    tEnv.executeSql("" +
        "INSERT INTO weibiao1 values \n" +
        "(0, 1000.1, 'a'),\n" +
        "(1, 1001.1, 'a'),\n" +
        "(2, 1002.1, 'a'),\n" +
        "(3, 1003.1, 'a'),\n" +
        "(4, 1004.1, 'a'),\n" +
        "(5, 1005.1, 'b'),\n" +
        "(6, 1006.1, 'b'),\n" +
        "(7, 1007.1, 'b'),\n" +
        "(8, 1008.1, 'b'),\n" +
        "(9, 1009.1, 'b');" +
        "").print();
    tEnv.executeSql("select * from weibiao1").print();
  }

  @Test
  public void sqlCreateTB4() {
    sqlCreateCatalog();
    tEnv.executeSql("" +
        "CREATE TABLE tb4(\n" +
        "  id int PRIMARY KEY NOT enforced,  -- 必须带，不然flink建表报错\n" +
        "  pre string, \n" +
        "  price double,\n" +
        "  par string\n" +
        ") WITH (\n" +
        "'connector' = 'hudi',\n" +
        "'table.type' = 'MERGE_ON_READ',\n" +
        "'write.operation' = 'insert',\n" +
        "'compaction.async.enabled' = 'false',\n" +
        "'hoodie.compact.inline' = 'true',\n" +
        "'compaction.delta_commits' = '1',\n" +
        "'write.tasks' = '1',\n" +
        "'compaction.tasks' = '1',\n" +
        "'hoodie.datasource.write.keygenerator.class' = 'org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator',  -- 非分区表必须带这个，否则后面spark插入会报错，默认是simple，\n" +
        "'precombine.field' = 'pre'  -- 必须带这个，不然spark建表报错，bug\n" +
        ");" +
        "").print();
    tEnv.executeSql("" +
        "INSERT INTO tb4 values \n" +
        "(0, 'n0', 0.1, 'a'),\n" +
        "(1, 'n1', 1.1, 'a'),\n" +
        "(2, 'n2', 2.1, 'a'),\n" +
        "(3, 'n3', 3.1, 'a'),\n" +
        "(4, 'n4', 4.1, 'a'),\n" +
        "(5, 'n5', 5.1, 'b'),\n" +
        "(6, 'n6', 6.1, 'b'),\n" +
        "(7, 'n7', 7.1, 'b'),\n" +
        "(8, 'n8', 8.1, 'b'),\n" +
        "(9, 'n9', 9.1, 'b');" +
        "").print();
  }
}
