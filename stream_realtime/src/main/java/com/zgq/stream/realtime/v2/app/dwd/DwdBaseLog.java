package com.zgq.stream.realtime.v2.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zgq.stream.realtime.v1.constant.Constant;
import com.zgq.stream.realtime.v1.utils.DateFormatUtil;
import com.zgq.stream.realtime.v1.utils.FlinkSinkUtil;
import com.zgq.stream.realtime.v1.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * @Package com.zgq.stream.realtime.v2.app.dwd.DwdBaseLog
 * @Author guoqiang.zhang
 * @Date 2025/5/4 14:38
 * @description:
 */

public class DwdBaseLog {

    private static final String START = "start";
    private static final String ERR = "err";
    private static final String DISPLAY = "display";
    private static final String ACTION = "action";
    private static final String PAGE = "page";

    public static void main(String[] args) throws Exception {
//        获取 Flink 的流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        设置并行度
        env.setParallelism(4);
//        开启检查点，5秒进行一次检查
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        消费kafka数据，指定kafka主题和消费者组
        KafkaSource <String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_LOG, "dwd_log");
//      读取kafka主题数据，创建DataStreamSource
        DataStreamSource <String> kafkaStrDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
//      打印kafka数据
//        kafkaStrDS.print();

//        定义脏数据侧输出流标签
        OutputTag <String> dirtyTag = new OutputTag<String>("dirtyTag"){};

//        对字符串进行处理，转为 JsonObject 类型
        SingleOutputStreamOperator < JSONObject > jsonObjDS = kafkaStrDS.process(
                new ProcessFunction <String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector <JSONObject> out) {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        } catch (Exception e) {
//                            若解析失败，将字符串输出到脏数据侧输出流
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
//        输出处理
//        打印解析后的json数据
//        jsonObjDS.print();

//        获取脏数据侧输出流
        SideOutputDataStream <String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
//      脏数据上传至 kafka 的 dirty_data 主题
        dirtyDS.sinkTo(FlinkSinkUtil.getKafkaSink("dirty_data"));
//        按照设备的mid进行分组，创建keyedStream
        KeyedStream <JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
//      对分组后的数据进行映射处理，修正is_new字段
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
//                定义 RichMapFunction 处理元素
                new RichMapFunction <JSONObject, JSONObject>() {
//                    设置状态变量，存储上一次访问日期
                    private ValueState <String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
//                        定义状态描述符
                        ValueStateDescriptor <String> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastVisitDateState", String.class);
//                        为状态设置过期时间，过期时间为 10 秒，在创建和写入时更新过期时间
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build());

//                        获取状态
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
//                        获取 is_new 字段
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
//                        获取上一次访问的日期
                        String lastVisitDate = lastVisitDateState.value();
//                        获取当前时间戳
                        Long ts = jsonObj.getLong("ts");
//                        将时间戳转化为字符串类型
                        String curVisitDate = DateFormatUtil.tsToDate(ts);


                        if ("1".equals(isNew)) {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterDay);
                            }
                        }

                        return jsonObj;
                    }
                }
        );
//        打印修正后的数据
//        fixedDS.print();

//        定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
//        分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
//                定义 ProcessFunction 处理元素
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) {
//                        获取错误日志
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            ctx.output(errTag, jsonObj.toJSONString());
//                            原始数据中移除报错信息
                            jsonObj.remove("err");
                        }

                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
//                            如果存在启动信息,数据输出至启动日志的测输出流
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else {
//                            页面日志
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");
//                            曝光日志
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject dispalyJsonObj = displayArr.getJSONObject(i);
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("display", dispalyJsonObj);
                                    newDisplayJsonObj.put("ts", ts);
//                                    曝光日志输出到测输出流
                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
//                                原始数据中移除曝光信息
                                jsonObj.remove("displays");
                            }

//                            动作日志
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
//                                    将动作日志输出到测输出流
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
//                                原始数据中移除动作信息
                                jsonObj.remove("actions");
                            }

//                            将处理后的页面日志收集到主输出流
                            out.collect(jsonObj.toJSONString());
                        }
                    }
                }
        );

//        获取测输出流
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        pageDS.print("页面:");
        errDS.print("错误:");
        startDS.print("启动:");
        displayDS.print("曝光:");
        actionDS.print("动作:");

//        存储不同类型的数据流到 Map 中
        Map<String, DataStream<String>> streamMap = new HashMap<>();
        streamMap.put(ERR,errDS);
        streamMap.put(START,startDS);
        streamMap.put(DISPLAY,displayDS);
        streamMap.put(ACTION,actionDS);
        streamMap.put(PAGE,pageDS);

//        将不同类型的测输出流存入到 kafka 主题
        streamMap
                .get(PAGE)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        streamMap
                .get(ERR)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        streamMap
                .get(START)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        streamMap
                .get(DISPLAY)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streamMap
                .get(ACTION)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));

//        执行 flink 任务
        env.execute("dwd_log");

    }

}