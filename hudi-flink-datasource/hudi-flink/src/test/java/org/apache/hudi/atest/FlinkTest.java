package org.apache.hudi.atest;

import com.google.common.collect.Lists;
import com.sun.corba.se.impl.corba.EnvironmentImpl;
import org.apache.avro.Schema;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.streamer.HoodieFlinkStreamer;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.spi.LoggerContext;
import org.apache.logging.slf4j.Log4jLoggerFactory;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.spi.LoggerContext;
import org.apache.logging.slf4j.Log4jLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Set;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class FlinkTest {


  /* mvn -U -pl hudi-flink-datasource -DskipTests -am clean package */
  /* mvn -pl hudi-flink-datasource -DskipTests -am clean package */

  static String tableName = "tb5";
  static String tablePath = ""
      + "file:///Users/wuwenchi/github/hudi/warehouse/database/"
      + tableName;
//  static String tablePath = ""
//      + "/tmp/hudi/warehouse/database/"
//      + tableName;
  static String pkField = "pk1";
  //  static String parField = "ts1";
  static String parField = "par1";
  static String preCom = "flong1";
  static int parall = 1;

  static StreamExecutionEnvironment env;
//  static TableEnvironmentImpl streamTableEnv;
  static StreamTableEnvironment tEnv;

  static {
    beforeClass();
  }

  public static void beforeClass() {
    createFlink();
    createTable();
  }

  public static void createFlink() {

    Configuration confData =
        new Configuration();
    confData.setString("akka.ask.timeout", "1h");
    confData.setString("akka.watch.heartbeat.interval", "1h");
    confData.setString("akka.watch.heartbeat.pause", "1h");
    confData.setString("heartbeat.timeout", "18000000");
    confData.setString(String.valueOf(CoreOptions.CHECK_LEAKED_CLASSLOADER), "false");
    confData.setString("rest.bind-port", "8080-9000");
    confData.setString(TableConfigOptions.LOCAL_TIME_ZONE, "UTC");
    env = StreamExecutionEnvironment.getExecutionEnvironment(confData);
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    env.enableCheckpointing(1000);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    env.setStateBackend(new HashMapStateBackend());
    env.getConfig().disableObjectReuse();
    // set up checkpoint interval
    env.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    env.setMaxParallelism(parall);
    env.setParallelism(parall);

    tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
//    tEnv.getConfig().getConfiguration()
//        .setInteger("sql-client.display.max-column-width", 10000000);
//    tEnv.getConfig().getConfiguration()
//        .setString("execution.runtime-mode", "streaming");
  }

  public static void createTable() {

    TestConfigurations.Sql tb1 = TestConfigurations.sql(tableName);
    Configuration hudiConf = getHudiConf();
    String sql = tb1

        .pkField(pkField)
//        .noPartition()
        .partitionField(parField)

//        .field("_hoodie_file_name string")

        .field("pk1 string")
        .field("pk2 string")

        .field("par1 string")
        .field("par2 string")

        .field("fint1 int")
        .field("flong1 bigint")

//        .field("ts1 timestamp(3)")
//        .field("ts2 timestamp(3)")

        .options(hudiConf.toMap())

        .end();

    tEnv.executeSql(sql);
  }


  public void insert(StreamTableEnvironment tEnv, String tableName) throws ExecutionException, InterruptedException {
    String s = "insert into " + tableName + " values \n" + getDataString();
//        + "('id8','id8','a','b',56,56000,TIMESTAMP '1970-01-01 00:00:06',TIMESTAMP '1970-01-01 00:00:08', x'0102030405060708090a0b0c0d0e0f10')";

    TableResult tableResult = tEnv.executeSql(s);
    tableResult.await();
  }

  @Test
  public void sqlWrite() throws ExecutionException, InterruptedException {
    insert(tEnv, tableName);
//    insert(tEnv, tableName);
  }

  @Test
  public void sqlRead() {
    CloseableIterator<Row> collect = tEnv.sqlQuery("select * from " + tableName).execute().collect();
    ArrayList<Row> rows = Lists.newArrayList(collect);
    System.out.println("============================================================");
    rows.forEach(System.out::println);
    System.out.println("============================================================");
//    tEnv.sqlQuery("select * from " + tableName).execute().print();
//    streamTableEnv.sqlQuery("select * from " + tableName + " where ts1 >= TIMESTAMP '1970-01-01 00:00:00' and ts1 <= TIMESTAMP '1970-01-02 00:00:00' ").execute().print();
//    tEnv.sqlQuery("select count(*) from " + tableName).execute().print();
  }

  static public Configuration getHudiConf() {

    String schema = "{\"type\":\"record\",\"name\":\"record\",\"fields\":[" +
        "{\"name\":\"pk1\",\"type\":\"string\"}," +
        "{\"name\":\"pk2\",\"type\":\"string\"}," +
        "{\"name\":\"par1\",\"type\":[\"null\",\"string\"],\"default\":null}," +
        "{\"name\":\"par2\",\"type\":[\"null\",\"string\"],\"default\":null}," +
        "{\"name\":\"fint1\",\"type\":[\"null\",\"int\"],\"default\":null}," +
        "{\"name\":\"flong1\",\"type\":[\"null\",\"long\"],\"default\":null}" +
//        "{\"name\":\"ts1\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}],\"default\":null}," +
//        "{\"name\":\"ts2\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}],\"default\":null}" +
        "]}\n";

    Configuration conf = new Configuration();
    conf.setString(FlinkOptions.PATH, tablePath);
    conf.setString(FlinkOptions.TABLE_NAME, tableName);
    conf.setString(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
//    conf.setString(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_COPY_ON_WRITE);
    conf.setBoolean(FlinkOptions.CHANGELOG_ENABLED, false);
    conf.setString(FlinkOptions.OPERATION, "upsert");
//    conf.setString(FlinkOptions.KEYGEN_TYPE, KeyGeneratorType.COMPLEX.name());

    // preCombine
    conf.setBoolean(FlinkOptions.PRE_COMBINE, true);
    conf.setString(FlinkOptions.PRECOMBINE_FIELD, preCom);

    // bucket index
    conf.setString(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    conf.setInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 2);
    conf.setString(FlinkOptions.INDEX_KEY_FIELD, "pk1");

    // key and partition
    conf.setString(FlinkOptions.KEYGEN_CLASS_NAME, ComplexAvroKeyGenerator.class.getName());
    conf.setString(FlinkOptions.RECORD_KEY_FIELD, pkField);
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, parField);

    // payload
    conf.setString(FlinkOptions.PAYLOAD_CLASS_NAME, EventTimeAvroPayload.class.getName());

    conf.setBoolean(FlinkOptions.INDEX_GLOBAL_ENABLED, false);

    // inline compaction
    conf.setBoolean(FlinkOptions.CLEAN_ASYNC_ENABLED, true);
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    conf.setBoolean("hoodie.compact.inline", true);
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 200);
//    conf.setInteger(FlinkOptions.CLUSTERING_DELTA_COMMITS, 2);

    // schema
    conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, schema);

    // write para
    conf.setInteger(FlinkOptions.WRITE_TASKS, parall);
    conf.setInteger(FlinkOptions.COMPACTION_TASKS, parall);
    conf.setInteger(FlinkOptions.BUCKET_ASSIGN_TASKS, parall);
    conf.setInteger(FlinkOptions.CLUSTERING_TASKS, parall);

    // read
    conf.setInteger(FlinkOptions.READ_TASKS, parall);

    return conf;
  }

  @Test
  public void streamWrite() throws Exception {

    Configuration conf = getHudiConf();

    // get schema and rowtype
    Schema sourceSchema = StreamerUtil.getSourceSchema(conf);
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(sourceSchema)
            .getLogicalType();
    System.out.println(rowType);

    // get datastream
    DataStream<RowData> dataStream = env.addSource(new DataSource());
    long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
    int parallelism = env.getParallelism();
    conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);

//    DataStream<HoodieRecord> hoodieRecordDataStream = Pipelines.bootstrap(conf, rowType, parallelism, dataStream);
//    DataStream<Object> pipeline = Pipelines.hoodieStreamWrite(conf, parallelism, hoodieRecordDataStream);
    DataStream<HoodieRecord> hoodieRecordDataStream = Pipelines.bootstrap(conf, rowType, dataStream);
    DataStream<Object> pipeline = Pipelines.hoodieStreamWrite(conf, hoodieRecordDataStream);
    if (OptionsResolver.needsAsyncCompaction(conf)) {
      Pipelines.compact(conf, pipeline);
    } else {
      Pipelines.clean(conf, pipeline);
    }

    env.execute();
  }

  static public GenericRowData[] getData() {
    return new GenericRowData[] {
        IDx_IDx_PARx_PARA(2,1,"a"),
        IDx_IDx_PARx_PARA(2,1,"a"),
        IDx_IDx_PARx_PARA(1,3,"a")
    };
  }

  static public String getDataString() {
    return Arrays.stream(getData())
        .map(
            data -> "(" +
                "'" + data.getString(0).toString() + "'," +
                "'" + data.getString(1).toString() + "'," +
                "'" + data.getString(2).toString() + "'," +
                "'" + data.getString(3).toString() + "'," +
                data.getInt(4) + "," +
                data.getLong(5)  + ")"
        )
        .collect(Collectors.joining(","));
  }


  @Test
  public void testDataString() {
    System.out.println(getDataString());
  }

  static public GenericRowData IDx_IDx_PARx_PARA(int id1, int id2, String par) {
    GenericRowData rowData = new GenericRowData(RowKind.INSERT, 6);

    rowData.setField(0, StringData.fromString("id"+id1));
    rowData.setField(1, StringData.fromString("id"+id2));

    rowData.setField(2, StringData.fromString(par));
    rowData.setField(3, StringData.fromString("a"));

    rowData.setField(4, 26);
    rowData.setField(5, 26L);
//      rowData.setField(6, TimestampData.fromEpochMillis(1000));
//      rowData.setField(7, TimestampData.fromEpochMillis(1000));
    return rowData;
  }

  public static class DataSource implements ParallelSourceFunction<RowData> {

    @Override
    public void run(SourceContext<RowData> ctx) throws InterruptedException {

      GenericRowData[] data = getData();
      int length = data.length - 1;
      for (GenericRowData datum : data) {
        ctx.collect(datum);
        System.out.println("wuwenchi ================== " + LocalDateTime.now());
        if (length > 0) {
          Thread.sleep(5000);
          length--;
        }
      }
    }

    @Override
    public void cancel() {

    }
  }


  @Test
  public void asdf() {
    ArrayDeque<Integer> objects = new ArrayDeque<>();
    for (int i = 0; i < 20; i++) {
      objects.push(i);
    }
    objects.push(1);
    objects.push(2);
    objects.push(3);
    objects.push(4);
    objects.pop();
    objects.pop();
    objects.pop();
    objects.pop();
  }

}
