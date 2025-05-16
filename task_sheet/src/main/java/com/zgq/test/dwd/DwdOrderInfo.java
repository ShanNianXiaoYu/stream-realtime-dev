package com.zgq.test.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zgq.stream.realtime.v1.utils.FlinkSinkUtil;
import com.zgq.stream.realtime.v1.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.zgq.app.dwd.DwdOrderInfo
 * @Author guoqiang.zhang
 * @Date 2025/5/14 19:21
 * @description: 订单信息实时清洗与关联作业
 */

public class DwdOrderInfo {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1（便于调试和单节点处理）
        env.setParallelism(1);

        // 从Kafka主题"topic_db"读取数据，消费者组为"dwd_app"
        KafkaSource<String> kafkaSourceBd = FlinkSourceUtil.getKafkaSource("topic_db", "dwd_app");
        // 读取Kafka数据，不使用水印
        DataStreamSource<String> kafka_source = env.fromSource(kafkaSourceBd, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 允许5秒乱序，使用ts_ms字段作为事件时间
        SingleOutputStreamOperator<JSONObject> streamOperator = kafka_source
                .map(JSON::parseObject) // 解析JSON字符串为JSONObject
                .assignTimestampsAndWatermarks( // 配置时间戳和水印
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // 允许5秒乱序
                                .withTimestampAssigner((element, recordTimestamp) -> element.getLong("ts_ms")) // 提取ts_ms作为时间戳
                );

        // 过滤出订单主表数据（table=order_info）
        SingleOutputStreamOperator<JSONObject> orderDs = streamOperator.filter(data ->
                data.getJSONObject("source").getString("table").equals("order_info"));

        // 过滤出订单明细表数据（table=order_detail）
        SingleOutputStreamOperator<JSONObject> detailDs = streamOperator.filter(data ->
                data.getJSONObject("source").getString("table").equals("order_detail"));

        // 处理订单主表数据：提取关键字段并格式化时间
        SingleOutputStreamOperator<JSONObject> operator = orderDs.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject rebuce = new JSONObject(); // 结果对象
                JSONObject after = value.getJSONObject("after"); // 变更后的数据

                // 提取订单主表字段
                rebuce.put("id", after.getString("id")); // 订单ID
                rebuce.put("user_id", after.getString("user_id")); // 用户ID
                rebuce.put("total_amount", after.getString("total_amount")); // 订单总金额

                // 处理创建时间：将时间戳转换为可读格式
                if (after != null && after.containsKey("create_time")) {
                    Long timestamp = after.getLong("create_time");
                    if (timestamp != null) {
                        // 时间戳转LocalDateTime
                        LocalDateTime dateTime = Instant.ofEpochMilli(timestamp)
                                .atZone(ZoneId.systemDefault())
                                .toLocalDateTime();
                        // 转换时间格式
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        rebuce.put("create_time", dateTime.format(formatter));
                    }
                }
                rebuce.put("ts_ms", value.getLong("ts_ms")); // 保留原始时间戳
                out.collect(rebuce);
            }
        });

        // 处理订单明细表数据：提取关键字段
        SingleOutputStreamOperator<JSONObject> detail = detailDs.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject rebuce = new JSONObject(); // 结果对象
                JSONObject after = value.getJSONObject("after"); // 变更后的数据

                // 提取订单明细表字段
                rebuce.put("id", after.getString("id")); // 明细ID
                rebuce.put("order_id", after.getString("order_id")); // 订单ID
                rebuce.put("sku_id", after.getString("sku_id")); // SKU ID
                rebuce.put("sku_name", after.getString("sku_name")); // SKU名称
                rebuce.put("sku_num", after.getString("sku_num")); // 商品数量
                rebuce.put("order_price", after.getString("order_price")); // 单价
                rebuce.put("split_activity_amount", value.getLong("split_activity_amount")); // 活动分摊金额
                rebuce.put("split_total_amount", value.getLong("split_total_amount")); // 总分摊金额
                rebuce.put("ts_ms", value.getLong("ts_ms")); // 保留原始时间戳
                out.collect(rebuce);
            }
        });

        // 按订单ID分组
        KeyedStream<JSONObject, String> idBy = operator.keyBy(data -> data.getString("id")); // 主表按订单ID分组
        KeyedStream<JSONObject, String> oidBy = detail.keyBy(data -> data.getString("order_id")); // 明细表按订单ID分组

        // 订单主表与明细表进行时间窗口内的JOIN
        SingleOutputStreamOperator<JSONObject> outputStreamOperator = idBy.intervalJoin(oidBy)
                .between(Time.minutes(-5), Time.minutes(5)) // 主表和明细表数据在5分钟内到达
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject merged = new JSONObject(); // 合并结果对象

                        // 合并订单主表字段
                        merged.put("id", left.getString("id")); // 订单ID
                        merged.put("user_id", left.getString("user_id")); // 用户ID
                        merged.put("total_amount", left.getString("total_amount")); // 订单总金额
                        merged.put("create_time", left.getString("create_time")); // 格式化后的创建时间

                        // 合并订单明细表字段
                        merged.put("detail_id", right.getLong("id")); // 明细ID
                        merged.put("order_id", right.getString("order_id")); // 订单ID（与主表ID一致）
                        merged.put("sku_id", right.getString("sku_id")); // SKU ID
                        merged.put("sku_name", right.getString("sku_name")); // SKU名称
                        merged.put("sku_num", right.getString("sku_num")); // 商品数量
                        merged.put("order_price", right.getString("order_price")); // 单价
                        merged.put("split_activity_amount", right.getLong("split_activity_amount")); // 活动分摊金额
                        merged.put("split_total_amount", right.getLong("split_total_amount")); // 总分摊金额
                        merged.put("ts_ms", right.getLong("ts_ms")); // 使用明细表的时间戳

                        out.collect(merged);
                    }
                });

        // 去重处理 保留每个明细ID的最新数据
        SingleOutputStreamOperator<JSONObject> operator1 = outputStreamOperator.keyBy(data -> data.getString("detail_id")) // 按明细ID分组
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<Long> latestTsState; // 存储最新时间戳的状态

                    @Override
                    public void open(Configuration parameters) {
                        // 配置状态生存时间1小时
                        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("latestTs", Long.class);
                        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.hours(1)).build());
                        latestTsState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        Long storedTs = latestTsState.value(); // 获取已存储的最新时间戳
                        long currentTs = value.getLong("ts_ms"); // 当前数据的时间戳

                        // 比较时间戳，仅保留最新数据
                        if (storedTs == null || currentTs > storedTs) {
                            latestTsState.update(currentTs); // 更新最新时间戳
                            out.collect(value); // 输出最新数据
                        }
                    }
                });

        operator1.print(); // 打印结果

        // 将结果写入Kafka主题"dwd_order_info_join"
        operator1.map(data -> data.toString()).sinkTo(FlinkSinkUtil.getKafkaSink("dwd_order_info_join"));

        // 执行Flink作业
        env.execute("DwdOrderInfo");
    }
}