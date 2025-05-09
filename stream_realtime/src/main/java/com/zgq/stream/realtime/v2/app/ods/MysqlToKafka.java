package com.zgq.stream.realtime.v2.app.ods;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.zgq.stream.realtime.v1.utils.FlinkSinkUtil;
import com.zgq.stream.realtime.v1.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.zgq.stream.realtime.v2.app.ods.MysqlToKafka
 * @Author guoqiang.zhang
 * @Date 2025/5/4 14:55
 * @description:
 */

public class MysqlToKafka {
 public static void main(String[] args) throws Exception {
  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  env.setParallelism(1);

//  从工具类获取MySQL数据源，数据源名称为realtime_v1，查询所有列
  MySqlSource <String> realtimeV1 = FlinkSourceUtil.getMySqlSource("realtime_v1", "*");

//  从MySQL数据源创建DataStreamSource，不使用水位线策略
  DataStreamSource <String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "MySQL Source");

//        mySQLSource.print();

//  使用工具类获取KafkaSink，目标Kafka主题为topic_db
  KafkaSink <String> topic_db = FlinkSinkUtil.getKafkaSink("topic_db");

//  将从MySQL读取的数据写入到Kafka的topic_db主题
  mySQLSource.sinkTo(topic_db);

  env.execute("Print MySQL Snapshot + Binlog");

 }
}