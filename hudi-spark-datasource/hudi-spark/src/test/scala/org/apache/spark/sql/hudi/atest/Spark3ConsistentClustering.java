package org.apache.spark.sql.hudi.atest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.clustering.plan.strategy.SparkConsistentBucketClusteringPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.SparkConsistentBucketClusteringExecutionStrategy;
import org.apache.hudi.client.clustering.update.strategy.SparkConsistentBucketDuplicateUpdateStrategy;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;
import org.apache.hudi.util.JFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class Spark3ConsistentClustering {
  protected JavaSparkContext jsc;
  protected HoodieSparkEngineContext context;
  protected SparkSession sparkSession;
  protected Configuration hadoopConf;
  protected SQLContext sqlContext;
  protected HoodieTableMetaClient metaClient;
  protected SparkRDDWriteClient writeClient;
  String tableName = "tb5";
  String pkField;
  String parField;
  HoodieWriteConfig config;

  void getSparkEnv() {
    SparkConf sparkConf = new SparkConf().setAppName("app")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[4]")
        .set("spark.sql.shuffle.partitions", "4")
        .set("spark.default.parallelism", "4");

    String evlogDir = System.getProperty("SPARK_EVLOG_DIR");
    if (evlogDir != null) {
      sparkConf.set("spark.eventLog.enabled", "true");
      sparkConf.set("spark.eventLog.dir", evlogDir);
    }
    sparkConf.set("spark.sql.hive.convertMetastoreParquet", "false");

    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest(  "app#method"));
    jsc.setLogLevel("ERROR");

    hadoopConf = jsc.hadoopConfiguration();
    context = new HoodieSparkEngineContext(jsc);

    Option<Consumer<SparkSessionExtensions>> sparkSessionExtensionsInjector = Option.empty();
    sparkSession = SparkSession.builder()
        .withExtensions(JFunction.toScala(sparkSessionExtensions -> {
          sparkSessionExtensionsInjector.ifPresent(injector -> injector.accept(sparkSessionExtensions));
          return null;
        }))
        .config(jsc.getConf())
        .getOrCreate();
    sqlContext = new SQLContext(sparkSession);
  }

  protected Properties getPropertiesForKeyGen() {
    Properties properties = new Properties();
    properties.put("hoodie.datasource.write.recordkey.field", pkField);
    properties.put("hoodie.datasource.write.partitionpath.field", parField);
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), pkField);
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), parField);
    properties.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), ComplexAvroKeyGenerator.class.getName());
    return properties;
  }

  public String getBasePath() {
    return "/Users/wuwenchi/github/hudi/warehouse/database/" + tableName;
  }

  public void setup(int maxFileSize) throws IOException {
    getSparkEnv();
    Properties props = getPropertiesForKeyGen();
    metaClient = HoodieTestUtils.init(hadoopConf, getBasePath(), HoodieTableType.MERGE_ON_READ, getPropertiesForKeyGen());
    config = getConfigBuilder().withProps(props)
        .withAutoCommit(false)
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.BUCKET).withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING)
            .withBucketNum("2").withBucketMaxNum(64).withBucketMinNum(2).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetMaxFileSize(maxFileSize).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(SparkConsistentBucketClusteringPlanStrategy.class.getName())
            .withClusteringExecutionStrategyClass(SparkConsistentBucketClusteringExecutionStrategy.class.getName())
            .withClusteringUpdatesStrategy(SparkConsistentBucketDuplicateUpdateStrategy.class.getName()).build())
        .build();

    writeClient = new SparkRDDWriteClient(context, config);
  }
  private HoodieWriteConfig.Builder getConfigBuilder() {
    String schema = "{\"type\":\"record\",\"name\":\"record\",\"fields\":[" +
      "{\"name\":\"pk1\",\"type\":\"int\"}," +
      "{\"name\":\"pk2\",\"type\":\"int\"}," +
      "{\"name\":\"par1\",\"type\":[\"null\",\"string\"],\"default\":null}," +
      "{\"name\":\"par2\",\"type\":[\"null\",\"string\"],\"default\":null}," +
      "{\"name\":\"fint1\",\"type\":[\"null\",\"int\"],\"default\":null}," +
      "{\"name\":\"pre1\",\"type\":[\"null\",\"long\"],\"default\":null}" +
      "]}\n";
    return HoodieWriteConfig.newBuilder().withPath(getBasePath()).withSchema(schema)
        .withParallelism(1, 1).withBulkInsertParallelism(1).withFinalizeWriteParallelism(1).withDeleteParallelism(1)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build());
  }


  @Test
  public void testSplit() throws IOException {
    tableName = "tb5";
    pkField = "pk1,pk2";
    parField = "par1";

    setup(512);

    String clusteringTime = (String) writeClient.scheduleClustering(Option.empty()).get();
    writeClient.cluster(clusteringTime, true);

    metaClient = HoodieTableMetaClient.reload(metaClient);
  }
}
