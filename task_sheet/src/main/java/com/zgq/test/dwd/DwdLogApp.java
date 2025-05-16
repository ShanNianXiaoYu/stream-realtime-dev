package com.zgq.test.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zgq.stream.realtime.v1.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @Package com.zgq.app.dwd.DwdLogApp
 * @Author guoqiang.zhang
 * @Date 2025/5/14 9:20
 * @description:
 */

public class DwdLogApp {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 流处理执行环境，并设置并行度为 1（便于调试和单节点处理）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从Kafka主题"topic_log"获取数据源，消费者组为"dwd_app"
        KafkaSource<String> kafkaSourceLog = FlinkSourceUtil.getKafkaSource("topic_log", "dwd_app");

        // 从Kafka Source读取数据，不使用水印（日志数据可能无需事件时间处理）
        SingleOutputStreamOperator<String> kafka_source_log = env.fromSource(kafkaSourceLog, WatermarkStrategy.noWatermarks(), "Kafka Source");
        // 将日志字符串解析为JSONObject，并分配时间戳（允许5秒乱序）
        SingleOutputStreamOperator<JSONObject> streamOperatorlog = kafka_source_log.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
               }));

        // 处理日志数据，提取设备信息和搜索词
        SingleOutputStreamOperator<JSONObject> logDeviceInfoDs = streamOperatorlog.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                JSONObject result = new JSONObject();
                if (jsonObject.containsKey("common")) {         // 提取公共字段（包含设备基础信息）
                    JSONObject common = jsonObject.getJSONObject("common");
                    // 处理用户ID：若为空则设为"-1"
                    result.put("uid", common.getString("uid") != null ? common.getString("uid") : "-1");
                    result.put("ts", jsonObject.getLongValue("ts"));
                    // 清洗设备信息：移除无关字段（sid/mid/is_new）
                    JSONObject deviceInfo = new JSONObject();
                    common.remove("sid");
                    common.remove("mid");
                    common.remove("is_new");
                    deviceInfo.putAll(common);       // 复制剩余字段到设备信息对象
                    result.put("deviceInfo", deviceInfo);

                    // 提取搜索词：仅当页面类型为"keyword"时有效
                    if (jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()) {
                        JSONObject pageInfo = jsonObject.getJSONObject("page");
                        if (pageInfo.containsKey("item_type") && pageInfo.getString("item_type").equals("keyword")) {
                            String item = pageInfo.getString("item");
                            result.put("search_item", item);
                        }
                    }
                }
                // 简化操作系统名称（例如："Android 11" → "Android"）
                JSONObject deviceInfo = result.getJSONObject("deviceInfo");
                String os = deviceInfo.getString("os").split(" ")[0];
                deviceInfo.put("os", os);

                return result;
            }
        });

//         过滤掉uid为空的无效数据
        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = logDeviceInfoDs.filter(data -> !data.getString("uid").isEmpty());
//         按用户ID分组，为后续状态操作做准备
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));
//         数据去重处理：使用状态存储已处理的JSON字符串，避免重复处理
        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private Logger LOG = LoggerFactory.getLogger(String.class);
            // 定义ValueState存储已处理的数据字符串集合（用于去重）
            private ValueState<HashSet<String>> processedDataState;

            @Override
            public void open(Configuration parameters) {
                // 初始化状态描述符，存储HashSet<String>类型的已处理数据
                ValueStateDescriptor<HashSet<String>> descriptor = new ValueStateDescriptor<>(
                        "processedDataState",
                        TypeInformation.of(new TypeHint<HashSet<String>>() {})
                );
                processedDataState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                // 从状态中获取已处理数据集合
                HashSet<String> processedData = processedDataState.value();
                if (processedData == null) {
                    processedData = new HashSet<>(); // 首次初始化空集合
                }

                String dataStr = value.toJSONString(); // 将数据转为字符串作为唯一标识
                LOG.info("Processing data: {}", dataStr);
                if (!processedData.contains(dataStr)) { // 检查是否为重复数据
                    LOG.info("Adding new data to set: {}", dataStr);
                    processedData.add(dataStr); // 添加新数据到集合
                    processedDataState.update(processedData); // 更新状态
                    out.collect(value); // 输出唯一数据
                } else {
                    LOG.info("Duplicate data found: {}", dataStr); // 打印重复日志（调试用）
                }
            }
        });
//        logDeviceInfoDs.print(); // 调试用：打印原始设备信息流

        // 窗口内聚合统计：按用户ID分组，统计2分钟内的PV、设备信息和搜索词
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(value -> value.getString("uid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    ValueState<Long> pvState; // 统计PV（页面访问量）
                    MapState<String, Set<String>> uvState; // 统计各字段的唯一值（UV相关）

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化PV状态（默认值为null，首次处理时设为1）
                        pvState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("pv", Long.class));

                        // 初始化UV状态：存储字段名到唯一值集合的映射
                        MapStateDescriptor<String, Set<String>> fieldsDescriptor =
                                new MapStateDescriptor<>("fields-state", Types.STRING, TypeInformation.of(new TypeHint<Set<String>>() {}));
                        uvState = getRuntimeContext().getMapState(fieldsDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 每次处理元素时PV自增
                        Long pv = pvState.value() == null ? 1L : pvState.value() + 1;
                        pvState.update(pv);

                        // 提取设备信息和搜索词
                        JSONObject deviceInfo = value.getJSONObject("deviceInfo");
                        String os = deviceInfo.getString("os"); // 操作系统
                        String ch = deviceInfo.getString("ch"); // 渠道
                        String md = deviceInfo.getString("md"); // 设备型号
                        String ba = deviceInfo.getString("ba"); // 品牌
                        String searchItem = value.containsKey("search_item") ? value.getString("search_item") : null; // 搜索词

                        // 更新各字段的唯一值集合
                        updateField("os", os);
                        updateField("ch", ch);
                        updateField("md", md);
                        updateField("ba", ba);
                        if (searchItem != null) {
                            updateField("search_item", searchItem);
                        }

                        // 构建输出结果：聚合各字段的唯一值
                        JSONObject output = new JSONObject();
                        output.put("uid", value.getString("uid")); // 用户ID
                        output.put("pv", pv); // 总访问次数
                        output.put("os", String.join(",", getField("os"))); // 操作系统列表
                        output.put("ch", String.join(",", getField("ch"))); // 渠道列表
                        output.put("md", String.join(",", getField("md"))); // 设备型号列表
                        output.put("ba", String.join(",", getField("ba"))); // 品牌列表
                        output.put("search_item", String.join(",", getField("search_item"))); // 搜索词列表

                        out.collect(output);
                    }

                    // 更新字段对应的唯一值集合
                    private void updateField(String field, String value) throws Exception {
                        Set<String> set = uvState.get(field) == null ? new HashSet<>() : uvState.get(field);
                        set.add(value); // 向集合中添加新值
                        uvState.put(field, set); // 更新状态
                    }

                    // 获取字段对应的唯一值集合（若无则返回空集合）
                    private Set<String> getField(String field) throws Exception {
                        return uvState.get(field) == null ? Collections.emptySet() : uvState.get(field);
                    }
                });

        // 滚动窗口聚合：每2分钟生成一次聚合结果，取窗口内最后一条数据
        SingleOutputStreamOperator<JSONObject> reduce = win2MinutesPageLogsDs.keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2))) // 2分钟滚动处理时间窗口
                .reduce((value1, value2) -> value2); // 保留窗口内最后一条记录（包含聚合结果）

        // 将JSON对象转换为字符串
        SingleOutputStreamOperator<String> operator = reduce.map(data -> data.toString());

        // 打印结果
        operator.print();

//        operator.sinkTo(FlinkSinkUtil.getKafkaSink("minutes_page_Log")); // 写入Kafka

        // 执行Flink作业
        env.execute("DwdLogApp");
    }
}
