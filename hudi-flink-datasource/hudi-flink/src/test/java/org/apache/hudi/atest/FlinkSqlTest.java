package org.apache.hudi.atest;

import org.junit.Test;

import java.util.ArrayList;

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
  public void sqlCreateWithPrimaryKey() {
    sqlCreateCatalog();
    tEnv.executeSql("" +
        "CREATE TABLE if not exists tb1(\n" +
        "  id int PRIMARY KEY NOT ENFORCED, \n" +
        "  name string, \n" +
        "  price double,\n" +
        "  par string\n" +
        ") PARTITIONED BY (par)\n" +
        "WITH (\n" +
        "'connector' = 'hudi',\n" +
//        "'precombine.field' = 'no_precombine',\n" +
        "'table.type' = 'MERGE_ON_READ' \n" +
        ");" +
        "");

    tEnv.executeSql("select * from tb1 ").print();
  }

  @Test
  public void qwerqwe() {
    ArrayList<String> strings = new ArrayList<>();
    strings.add("asdf");
    System.out.println(strings.contains(null));
    System.out.println(strings.contains("asdf"));
  }
}
