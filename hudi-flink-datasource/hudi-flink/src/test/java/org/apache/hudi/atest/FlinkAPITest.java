package org.apache.hudi.atest;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.junit.jupiter.api.Test;

public class FlinkAPITest extends FlinkTest {
  @Test
  public void streamWrite() throws Exception {

    Configuration conf = getHudiConf();

    // get schema and rowtype
    Schema sourceSchema = StreamerUtil.getSourceSchema(conf);
    RowType rowType = (RowType) AvroSchemaConverter.convertToDataType(sourceSchema).getLogicalType();
    System.out.println(rowType);

    // get datastream
    DataStream<RowData> dataStream = env.addSource(new FlinkTest.DataSource());
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
}
