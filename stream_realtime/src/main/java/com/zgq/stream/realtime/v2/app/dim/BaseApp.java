package com.zgq.stream.realtime.v2.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.zgq.stream.realtime.v1.bean.TableProcessDim;
import com.zgq.stream.realtime.v1.constant.Constant;
import com.zgq.stream.realtime.v1.function.HBaseSinkFunction;
import com.zgq.stream.realtime.v1.function.TableProcessFunction;
import com.zgq.stream.realtime.v1.utils.FlinkSourceUtil;
import com.zgq.stream.realtime.v1.utils.HBaseUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package com.zgq.stream.realtime.v2.app.dim.BaseApp
 * @Author guoqiang.zhang
 * @Date 2025/5/4 14:35
 * @description:
 */

public class BaseApp {
    public static void main(String[] args) throws Exception {
//        创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        设置并行度为 4
        env.setParallelism(4);
//        设置检查点 5秒检查一次
        env.enableCheckpointing(5000L , CheckpointingMode.EXACTLY_ONCE);
//        消费 kafka 数据,定义消费者组id dim_app
        KafkaSource <String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dim_app");
//        获取 kafka 主题,不设置检查点
        DataStreamSource <String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

//        kafkaStrDS.print();

//        对 Kafka 数据进行处理，转换为 JSONObject 并进行过滤
        SingleOutputStreamOperator < JSONObject > jsonObjDS = kafkaStrDS.process(
                new ProcessFunction <String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector <JSONObject> out) {
//                        将字符串解析为 JSONObject
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
//                        从 JSONObject 中获取数据库名称
                        String db = jsonObj.getJSONObject("source").getString("db");
//                        从 JSONObject 中获取操作类型
                        String type = jsonObj.getString("op");
//                        从 JSONObject 中获取数据
                        String data = jsonObj.getString("after");

//                        根据条件进行过滤
                        if ("realtime_v1".equals(db)
                                && ("c".equals(type)
                                || "u".equals(type)
                                || "d".equals(type)
                                || "r".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
//                            满足条件则收集数据
                            out.collect(jsonObj);
                        }
                    }
                }
        );


//        jsonObjDS.print();

//        获取 MySQL 数据源，指定数据库名称和表名
        MySqlSource <String> mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v2", "table_process_dim");
//        从 MySQL 数据源创建 DataStreamSource，不设置水位线，设置并行度为 1
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);

//        将 MySQL 数据映射为 TableProcessDim 对象，并设置操作类型
        SingleOutputStreamOperator< TableProcessDim > tpDS = mysqlStrDS.map(
//                定义 MapFunction 将 MySQL 数据转换为 TableProcessDim 对象
                new MapFunction <String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim;
                        if("d".equals(op)){
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        }else{
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);

//        tpDS.print();

//        对 TableProcessDim 数据进行处理，与 HBase 交互
        tpDS.map(
                new RichMapFunction <TableProcessDim, TableProcessDim>() {

                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
//                        初始化时获取 HBase 连接
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
//                        关闭时关闭 HBase 连接
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) {
                        String op = tp.getOp();
                        String sinkTable = tp.getSinkTable();
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if("d".equals(op)){
//                            如果操作类型为删除，删除 HBase 表
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable);
                        }else if("r".equals(op)||"c".equals(op)){
//                            如果操作类型为读取或创建，创建 HBase 表
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }else{
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);

//        tpDS.print();

//        定义一个 MapStateDescriptor，用于广播状态
        MapStateDescriptor <String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<>("mapStateDescriptor",String.class, TableProcessDim.class);
//        数据广播到所有并行任务
        BroadcastStream <TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);
//        将过滤后的 JSON 对象数据与广播数据连接
        BroadcastConnectedStream <JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);
//        处理连接后的数据
        SingleOutputStreamOperator< Tuple2 <JSONObject,TableProcessDim> > dimDS = connectDS.process(new TableProcessFunction(mapStateDescriptor));
//        打印处理后的数据
        dimDS.print();
//        将处理后数据存入 HBase
        dimDS.addSink(new HBaseSinkFunction());
//        执行 flink 任务
        env.execute("dim");

    }
}
