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

    // 默认，两个主键，一个分区
    sqlCreateCatalog();

    String ddl = getDDLString();
//    System.out.println(ddl);
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
        .field("pre1 bigint")

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
    tableName = "tb5";
    sqlCreate();
    sqlInsertInto();
    sqlRead();
  }

}

/*

无分区、两次提交做一次合并的表：
  CREATE TABLE tb7(
    id int PRIMARY KEY NOT enforced,  -- 必须带，不然flink建表报错
    price double,
    pre string
  ) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'write.operation' = 'insert',
    'compaction.async.enabled' = 'false',
    'hoodie.compact.inline' = 'true',
    'compaction.delta_commits' = '2',
    'write.tasks' = '1',
    'compaction.tasks' = '1',
    'hoodie.datasource.write.keygenerator.class' = 'org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator',  -- 非分区表必须带这个，否则后面spark插入会报错，默认是simple，
    'precombine.field' = 'pre'  -- 必须带这个，不然spark建表报错，bug
  );


  创建buckindex类型的表
CREATE TABLE IF NOT EXISTS tb9(
  pk1 int,
  pk2 int,
  par1 string,
  par2 string,
  fint1 int,
  pre1 bigint,
  PRIMARY KEY(pk1, pk2) NOT ENFORCED
)
PARTITIONED BY (par1)
with (
  'connector' = 'hudi',
  'index.type' = 'BUCKET',
  'write.bucket_assign.tasks' = '1',
  'hoodie.bucket.index.hash.field' = 'pk1',
  'metadata.enabled' = 'false',
  'write.operation' = 'upsert',
  'precombine.field' = 'pre1',
  'hoodie.bucket.index.num.buckets' = '32',
  'table.type' = 'MERGE_ON_READ',
  'write.tasks' = '1'
);

 */
