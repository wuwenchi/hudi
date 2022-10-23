package org.apache.hudi.atest;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.HoodiePipeline;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
//  static String parField = "par1,par2";
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
        .noPartition()
//        .partitionField(parField)

        .field("_hoodie_file_name string")

        .field("pk1 string")
        .field("pk2 string")

//        .field("par1 string")
        .field("par2 string")

        .field("fint1 int")
        .field("flong1 bigint")
        .field("par1 string")

//        .field("ts1 timestamp(3)")
//        .field("ts2 timestamp(3)")

        .options(hudiConf.toMap())

        .end();

    tEnv.executeSql(sql);
  }

  @Test
  public void testCreate() {
    ThreadLocal<String> local = new ThreadLocal<>();
    ThreadLocal<String> local2 = new ThreadLocal<>();

    IntStream.range(0, 10).forEach(i -> new Thread(() -> {
      local.set(Thread.currentThread().getName() + ":" + i);
      local2.set("werwerwer");
      System.out.println("线程：" + Thread.currentThread().getName() + ",local:" + local.get());
    }).start());
    ConcurrentHashMap<String, String> stringStringConcurrentHashMap = new ConcurrentHashMap<>();
//    stringStringConcurrentHashMap.put()
  }

  @Test
  public void testBlock() throws InterruptedException {
    BlooooockQueue queue = new BlooooockQueue();
    new Thread(() -> {
      try {
        System.out.println("添加");
        queue.put(11);
        queue.put(12);
        queue.put(13);
        queue.put(14);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }).start();
    new Thread(() -> {
      try {
        System.out.println("取出");
        queue.take();
        Thread.sleep(1);
        queue.take();
        queue.take();
        queue.take();
        queue.take();
        queue.take();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }).start();
  }

  static class BlooooockQueue extends Object {
    Object[] obj;
    private int head = 0;
    private int tail = 0;
    ReentrantLock lock = new ReentrantLock();
    private final Condition empty = lock.newCondition();
    private final Condition have = lock.newCondition();
    private final int size;

    public BlooooockQueue(int size) {
      this.obj = new Object[size];
      this.size = size;
    }
    public BlooooockQueue() {
      this.obj = new Object[3];
      this.size = 3;
    }
    public int take() throws InterruptedException {

      lock.lock();
      try {
        if (head == tail) {
          System.out.println(" empty ================================");
          // empty, wait obj
          have.await();
        }
        int ret = (int) this.obj[head];
        head = (head + 1) % size;
        empty.signal();
        System.out.println("take " + ret);
        return ret;
      } finally {
        lock.unlock();
      }
    }
    public void put(int obj) throws InterruptedException {
      lock.lock();
      System.out.println("put " + obj);
      try {
        if ((tail + 1) % size == head) {
          // full, wait empty
          System.out.println(" full ================================");
          empty.await();
        }
        this.obj[tail] = obj;
        tail = (tail + 1) % size;
        have.signal();
      } finally {
        lock.unlock();
      }
    }
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
    CloseableIterator<Row> collect = tEnv.sqlQuery("select _hoodie_file_name,pk1,pk2 from " + tableName).execute().collect();
    ArrayList<Row> rows = Lists.newArrayList(collect);
    System.out.println("============================================================");
    rows.forEach(System.out::println);
    System.out.println("============================================================");
//    tEnv.sqlQuery("select * from " + tableName).execute().print();
//    streamTableEnv.sqlQuery("select * from " + tableName + " where ts1 >= TIMESTAMP '1970-01-01 00:00:00' and ts1 <= TIMESTAMP '1970-01-02 00:00:00' ").execute().print();
//    tEnv.sqlQuery("select count(*) from " + tableName).execute().print();
//    tEnv.sqlQuery("select _hoodie_file_name,pk1,pk2 from " + tableName).execute().print();
  }

  @Test
  public void streamRead() throws Exception {

    HoodiePipeline.Builder builder = HoodiePipeline.builder(tableName)
        .column("pk1 string")
        .column("pk2 string")
        .column("par1 string")
        .column("par2 string")
        .column("fint1 int")
        .column("flong1 bigint")
        .pk(pkField)
        .partition(parField)
        .options(getHudiConf().toMap());

    DataStream<RowData> rowDataDataStream = builder.source(env);
    rowDataDataStream.print();
    env.execute("Api_Source");
//    System.out.println("===================================================");
//    CloseableIterator<RowData> rowDataCloseableIterator = rowDataDataStream.executeAndCollect();
//    rowDataCloseableIterator.forEachRemaining(System.out::println);
//    System.out.println("===================================================");
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
//    conf.setString(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
//    conf.setInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 2);
//    conf.setString(FlinkOptions.INDEX_KEY_FIELD, "pk1");

    // key and partition
    conf.setString(FlinkOptions.KEYGEN_CLASS_NAME, ComplexAvroKeyGenerator.class.getName());
//    conf.setString(FlinkOptions.KEYGEN_TYPE, KeyGeneratorType.COMPLEX.name());
    conf.setString(FlinkOptions.RECORD_KEY_FIELD, pkField);
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, parField);

    // payload
    conf.setString(FlinkOptions.PAYLOAD_CLASS_NAME, EventTimeAvroPayload.class.getName());

    conf.setBoolean(FlinkOptions.INDEX_GLOBAL_ENABLED, false);

    // inline compaction
    conf.setBoolean(FlinkOptions.CLEAN_ASYNC_ENABLED, true);
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    conf.setBoolean("hoodie.compact.inline", true);
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 2);
//    conf.setInteger(FlinkOptions.CLUSTERING_DELTA_COMMITS, 2);

    // metadata table
    conf.setBoolean(FlinkOptions.METADATA_ENABLED, false);

    // schema
    conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, schema);

    // write para
    conf.setInteger(FlinkOptions.WRITE_TASKS, parall);
    conf.setInteger(FlinkOptions.COMPACTION_TASKS, parall);
    conf.setInteger(FlinkOptions.BUCKET_ASSIGN_TASKS, parall);
    conf.setInteger(FlinkOptions.CLUSTERING_TASKS, parall);

    // read
    conf.setInteger(FlinkOptions.READ_TASKS, parall);

    // commit begin - end
//    conf.setString(FlinkOptions.READ_START_COMMIT, "20220919112526"); // specifies the start commit instant time
//    conf.setString(FlinkOptions.READ_END_COMMIT, "20220919112526"); // specifies the start commit instant time

    // stream read
//    conf.setBoolean(FlinkOptions.READ_AS_STREAMING, true); // this option enable the streaming read

    // index
    conf.setBoolean(HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER.key(), true);
    conf.setInteger(HoodieMetadataConfig.METADATA_INDEX_BLOOM_FILTER_FILE_GROUP_COUNT.key(), 1);

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
//    if (OptionsResolver.needsAsyncCompaction(conf)) {
//      Pipelines.compact(conf, pipeline);
//    } else {
//      Pipelines.clean(conf, pipeline);
//    }

    env.execute();
  }

  static public GenericRowData[] getData() {
    return new GenericRowData[] {
//        IDx_IDx_PARx_PARA(2,1,"a"),
//        IDx_IDx_PARx_PARA(11,10,"b"),
//        IDx_IDx_PARx_PARA(10,10,"b"),
//        IDx_IDx_PARx_PARA(9,10,"a"),
        IDx_IDx_PARx_PARA(4,11,"a")
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
//        if (length > 0) {
//          Thread.sleep(5000);
//          length--;
//        }
      }
    }

    @Override
    public void cancel() {
    }
  }

  @Test
  public void testList() {
    LinkedList<String> strings = new LinkedList<>();

    int size = strings.size();
    strings.add("a");
    strings.add("b");
    strings.add("c");
    strings.forEach(System.out::println);
    String pop = strings.pop();
//    System.out.println(pop);
    strings.forEach(System.out::println);
  }

  @Test
  public void testMultiArray() {
    int[] aaa = new int[2];
    aaa[0] = 1;
    System.out.println(aaa);

    int[] aa = new int[] {1,2,3};
    System.out.println(aa);
    int[] aa2 = {2,3,4};

    // ===========================================================
    // 直接赋值
    int[][] inte = {
        {1, 2, 4},
        {3, 4}
    };
    for (int i = 0; i < inte.length; i++) {
      for (int j = 0; j < inte[i].length; j++) {
        System.out.println(inte[i][j]);
      }
    }

    // ===========================================================
    // 先创建好引用
    int[][] ints3 = new int[4][5];
    for (int[] i :
        ints3) {
      for (int j :
          i) {
        System.out.println(j);
      }
    }
    // ===========================================================
    // 一个一个创建
    int[][] ints1 = new int[2][];
    int[] ints = new int[3];
    int[] ints2 = new int[4];
    ints1[0] = ints;
    ints1[1] = ints2;
    System.out.println(ints1[0].length + "  " + ints1[1].length);
  }

}
