package com.zgq.stream.realtime.v2.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zgq.stream.realtime.v1.bean.CartAddUuBean;
import com.zgq.stream.realtime.v1.function.BeanToJsonStrMapFunction;
import com.zgq.stream.realtime.v1.utils.DateFormatUtil;
import com.zgq.stream.realtime.v1.utils.FlinkSinkUtil;
import com.zgq.stream.realtime.v1.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.zgq.stream.realtime.v2.app.dws.DwsTradeCartAddUuWindow
 * @Author guoqiang.zhang
 * @Date 2025/5/4 14:42
 * @description:
 */


/**
 * @Package com.zgq.stream.realtime.v2.app.dws.DwsTradeCartAddUuWindow
 * @Author guoqiang.zhang
 * @Date 2025/5/4 14:42
 * @description:
 */

public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 设置检查点间隔 5000 ms ,精确一次
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 设置重启策略为固定延迟重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));

//        设置消费者组件消费kafka主题
        KafkaSource <String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_trade_cart_add", "dws_trade_cart_add_uu_window");

//        从 Kafka 数据源创建数据流，不使用水位线策略
        DataStreamSource <String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
//        将字符串类型的数据映射为JSONObject类型的数据
        SingleOutputStreamOperator < JSONObject > jsonObjDS = kafkaStrDS.map(JSON::parseObject);
//        为数据流分配时间戳和水位线，使用单调递增时间戳策略
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner <JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts_ms") * 1000;
                                    }
                                }
                        )
        );
//        根据user_id对数据进行分组
        KeyedStream <JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));

//        对分组后的数据进行处理
        SingleOutputStreamOperator<JSONObject> cartUUDS = keyedDS.process(
                new KeyedProcessFunction <String, JSONObject, JSONObject>() {
//                    存储上次加购状态
                    private ValueState <String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor <String> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastCartDateState", String.class);
//                        设置状态的生命周期 1天
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
//                        获取状态
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector <JSONObject> out) throws Exception {
                        //从状态中获取上次加购日期
                        String lastCartDate = lastCartDateState.value();
                        //获取当前这次加购日期
                        Long ts = jsonObj.getLong("ts_ms");
                        String curCartDate = DateFormatUtil.tsToDate(ts);
                        if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                            out.collect(jsonObj);
                            lastCartDateState.update(curCartDate);
                        }
                    }
                }
        );

//        对处理后的数据进行窗口操作，使用滚动事件时间窗口，窗口大小为2秒
        AllWindowedStream <JSONObject, TimeWindow > windowDS = cartUUDS
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(2)));

//        对窗口内的数据进行聚合操作
        SingleOutputStreamOperator<CartAddUuBean> aggregateDS = windowDS.aggregate(
                new AggregateFunction <JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject value, Long accumulator) {
                        return ++accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                },
                new AllWindowFunction <Long, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) {
//                        获取窗口内聚合后的数量
                        Long cartUUCt = values.iterator().next();
//                        获取窗口的开始时间戳并转换为秒
                        long startTs = window.getStart() / 1000;
//                        获取窗口的结束时间戳并转换为秒
                        long endTs = window.getEnd() / 1000;
//                        将开始时间戳转换为日期时间字符串
                        String stt = DateFormatUtil.tsToDateTime(startTs);
//                        将结束时间戳转换为日期时间字符串
                        String edt = DateFormatUtil.tsToDateTime(endTs);
//                        将开始时间戳转换为日期字符串
                        String curDate = DateFormatUtil.tsToDate(startTs);
//                        输出聚合后的结果
                        out.collect(new CartAddUuBean(
                                stt,
                                edt,
                                curDate,
                                cartUUCt
                        ));
                    }
                }
        );

//        将聚合后的结果映射为字符串类型
        SingleOutputStreamOperator<String> operator = aggregateDS
                .map(new BeanToJsonStrMapFunction <>());

        operator.print();
//        处理后的数据写入doris数据库
        operator.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));

        env.execute("DwsTradeCartAddUuWindow");
    }
}
