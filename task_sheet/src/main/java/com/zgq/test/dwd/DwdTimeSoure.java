package com.zgq.test.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zgq.stream.realtime.v1.bean.DimBaseCategory;
import com.zgq.stream.realtime.v1.bean.DimSkuInfoMsg;
import com.zgq.stream.realtime.v1.utils.FlinkSourceUtil;
import com.zgq.stream.realtime.v1.utils.JdbcUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.time.Duration;
import java.util.*;

/**
 * @Package com.zgq.app.dwd.DwdTimeSoure
 * @Author guoqiang.zhang
 * @Date 2025/5/15 9:14
 * @description:
 */

/**
 * 实时数据处理类：从Kafka读取订单数据，关联维度信息并进行特征打分
 */
public class DwdTimeSoure {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1（测试环境常用配置，生产环境需根据集群资源调整）
        env.setParallelism(1);

        // 从工具类获取Kafka数据源（参数：主题名、消费者组名）
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_order_info_join", "DwdTimeSoure");
        // 从数据源创建DataStream，不生成水位线（适用于无序或不需要事件时间的场景）
        DataStreamSource<String> fromSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        // 数据处理流程：
        // 1. 解析JSON字符串为JSONObject
        // 2. 分配时间戳和水位线（允许5秒乱序）
        SingleOutputStreamOperator<JSONObject> streamOperator = fromSource
                .map(JSON::parseObject) // 将Kafka消息转换为JSON对象
                .assignTimestampsAndWatermarks( // 配置水位线策略和时间戳提取
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // 允许最大5秒乱序
                                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        // 从JSON中提取时间戳字段（单位：毫秒）
                                        return element.getLong("ts_ms");
                                    }
                                })
                );

        // 富函数Map处理：关联维度表并进行特征打分（包含数据库连接和状态管理）
        SingleOutputStreamOperator<JSONObject> operator1 = streamOperator.map(new RichMapFunction<JSONObject, JSONObject>() {
            private Connection connection; // MySQL数据库连接
            private List< DimSkuInfoMsg > dimSkuInfoMsgs; // SKU维度信息列表（包含SPU、类目、品牌信息）
            private List< DimBaseCategory > dimBaseCategories; // 类目层级关联结果（三级类目-二级类目-一级类目）
            private List<DimBaseCategory> dim_base_categories; // 别名，保持与SQL查询结果变量名一致
            private double timeRate = 0.1; // 时间特征权重
            private double amountRate = 0.15; // 金额特征权重
            private double brandRate = 0.2; // 品牌特征权重
            private double categoryRate = 0.3; // 类目特征权重

            /**
             * 初始化方法：仅执行一次，用于获取数据库连接和预加载维度数据
             * 注：生产环境建议使用缓存（如Redis）或维表JOIN替代全量查询
             */
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 获取MySQL数据库连接（工具类封装）
                connection = JdbcUtil.getMySQLConnection();

                // 查询类目层级关联数据（三级类目 -> 二级类目 -> 一级类目）
                String sql1 = "  SELECT                                                        \n" +
                        "   b3.id, b3.name b3name, b2.name b2name, b1.name b1name     \n" +
                        "   FROM realtime_v1.base_category3 as b3                             \n" +
                        "   JOIN realtime_v1.base_category2 as b2                             \n" +
                        "   ON b3.category2_id = b2.id                                         \n" +
                        "   JOIN realtime_v1.base_category1 as b1                             \n" +
                        "   ON b2.category1_id = b1.id                                         ";

                // 执行查询并映射到DimBaseCategory对象列表（工具类方法，自动关闭结果集）
                dim_base_categories = JdbcUtil.queryList(connection, sql1, DimBaseCategory.class, false);

                // 查询SKU维度信息（关联SPU和品牌）
                String querySkuSql = "  select sku_info.id AS id,                                     \n" +
                        "                   spu_info.id AS spuid,                                            \n" +
                        "                   spu_info.category3_id AS c3id,                                   \n" +
                        "                   base_trademark.tm_name AS name                                   \n" +
                        "                   from realtime_v1.sku_info                                        \n" +
                        "                   join realtime_v1.spu_info                                        \n" +
                        "                   on sku_info.spu_id = spu_info.id                                 \n" +
                        "                   join realtime_v1.base_trademark                                  \n" +
                        "                   on realtime_v1.spu_info.tm_id = realtime_v1.base_trademark.id   ";
                // 预加载SKU维度数据到内存
                dimSkuInfoMsgs = JdbcUtil.queryList(connection, querySkuSql, DimSkuInfoMsg.class);

                // 调试输出：打印所有预加载的SKU维度数据（生产环境可移除）
                for (DimSkuInfoMsg dimSkuInfoMsg : dimSkuInfoMsgs) {
                    System.err.println(dimSkuInfoMsg);
                }
            }

            /**
             * 核心数据处理逻辑：每条数据的映射转换
             * @param jsonObject 输入的订单数据（JSON格式）
             * @return 包含维度信息和特征打分的JSON对象
             */
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                // 1. 关联SKU维度信息（通过sku_id查找对应的类目ID和品牌名称）
                String skuId = jsonObject.getString("sku_id");
                if (skuId != null && !skuId.isEmpty()){
                    // 遍历预加载的SKU维度列表进行匹配
                    for (DimSkuInfoMsg dimSkuInfoMsg : dimSkuInfoMsgs) {
                        if (dimSkuInfoMsg.getId().equals(skuId)){
                            // 注入三级类目ID和品牌名称
                            jsonObject.put("c3id", dimSkuInfoMsg.getCategory3_id());
                            jsonObject.put("tname", dimSkuInfoMsg.getTm_name());
                            break; // 找到匹配项后跳出循环
                        }
                    }
                }

                // 2. 关联类目层级信息（通过三级类目ID查找一级类目名称）
                String c3id = jsonObject.getString("c3id");
                if (c3id != null && !c3id.isEmpty()){
                    // 遍历预加载的类目层级列表
                    for (DimBaseCategory dimBaseCategory : dim_base_categories) {
                        if (c3id.equals(dimBaseCategory.getId())){
                            // 注入一级类目名称
                            jsonObject.put("b1_name", dimBaseCategory.getB1name());
                            break;
                        }
                    }
                }

                // 3. 时间特征打分（根据订单创建时间所属时段分配不同权重）
                String payTimeSlot = jsonObject.getString("create_time"); // 时间时段字段（如"晚上"、"夜间"等）
                if (payTimeSlot != null && !payTimeSlot.isEmpty()){
                    switch (payTimeSlot) {
                        // 不同时段对应不同特征项的分值（基于timeRate权重计算）
                        case "凌晨":
                            // 示例：凌晨时段各时间特征项的分值计算
                            jsonObject.put("pay_time_18-24", round(0.2 * timeRate));
                            jsonObject.put("pay_time_25-29", round(0.1 * timeRate));
                            // 省略部分相似代码（逻辑一致，仅数值不同）
                            break;
                        case "早晨":
                            // 早晨时段分值分配
                            jsonObject.put("pay_time_18-24", round(0.1 * timeRate));
                            jsonObject.put("pay_time_25-29", round(0.1 * timeRate));
                            break;
                        // 省略其他时段的case分支（逻辑与上述类似）
                        case "夜间":
                            // 夜间时段分值分配
                            jsonObject.put("pay_time_18-24", round(0.9 * timeRate));
                            jsonObject.put("pay_time_25-29", round(0.7 * timeRate));
                            break;
                    }
                }

                // 4. 金额特征打分（根据订单总金额划分区间计算分值）
                double totalAmount = jsonObject.getDoubleValue("total_amount");
                if (totalAmount < 1000){
                    // 低金额区间分值
                    jsonObject.put("amount_18-24", round(0.8 * amountRate));
                    jsonObject.put("amount_25-29", round(0.6 * amountRate));
                } else if (totalAmount > 1000 && totalAmount < 4000) {
                    // 中金额区间分值
                    jsonObject.put("amount_18-24", round(0.2 * amountRate));
                    jsonObject.put("amount_25-29", round(0.4 * amountRate));
                } else {
                    // 高金额区间分值
                    jsonObject.put("amount_18-24", round(0.1 * amountRate));
                    jsonObject.put("amount_25-29", round(0.2 * amountRate));
                }

                // 5. 品牌特征打分（根据品牌名称匹配预设分值）
                String tname = jsonObject.getString("tname");
                if (tname != null && !tname.isEmpty()){
                    switch (tname) {
                        case "TCL":
                            // 特定品牌分值分配
                            jsonObject.put("tname_18-24", round(0.2 * brandRate));
                            jsonObject.put("tname_25-29", round(0.3 * brandRate));
                            break;
                        case "苹果":
                        case "联想":
                        case "小米":
                            // 多个品牌共享相同分值
                            jsonObject.put("tname_18-24", round(0.9 * brandRate));
                            jsonObject.put("tname_25-29", round(0.8 * brandRate));
                            break;
                        // 省略其他品牌的case分支（逻辑与上述类似）
                        default:
                            // 未匹配品牌的默认分值
                            jsonObject.put("tname_18-24", round(0.1 * brandRate));
                            jsonObject.put("tname_25-29", round(0.2 * brandRate));
                            break;
                    }
                }

                // 6. 类目特征打分（根据一级类目名称匹配预设分值）
                String b1Name = jsonObject.getString("b1_name");
                if (b1Name != null && !b1Name.isEmpty()){
                    switch (b1Name){
                        case "数码":
                        case "手机":
                        case "电脑办公":
                            // 热门类目分值分配
                            jsonObject.put("b1name_18-24", round(0.9 * categoryRate));
                            jsonObject.put("b1name_25-29", round(0.8 * categoryRate));
                            break;
                        case "家居家装":
                        case "图书、音像、电子书刊":
                            // 非热门类目分值分配
                            jsonObject.put("b1name_18-24", round(0.2 * categoryRate));
                            jsonObject.put("b1name_25-29", round(0.4 * categoryRate));
                            break;
                        // 省略其他类目的case分支（逻辑与上述类似）
                        default:
                            // 未知类目的默认分值
                            jsonObject.put("b1name_18-24", round(0.1 * categoryRate));
                            jsonObject.put("b1name_25-29", round(0.2 * categoryRate));
                    }
                }

                return jsonObject; // 返回处理后的JSON对象
            }

            /**
             * 资源释放方法：关闭数据库连接（与open方法中的资源获取对应）
             */
            @Override
            public void close() throws Exception {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            }
        });

        // 打印输出处理后的数据（测试用，生产环境建议注释或替换为正式Sink）
        operator1.print();

        // 待启用的KafkaSink（当前代码被注释，需配置FlinkSinkUtil后使用）
        // operator1.map(data -> data.toString()).sinkTo(FlinkSinkUtil.getKafkaSink("dwd_time_soure"));

        // 触发流计算作业执行（阻塞方法，作业会一直运行直至停止）
        env.execute("DwdTimeSoure");
    }

    /**
     * 数值四舍五入工具方法：保留3位小数
     * @param value 待处理的数值
     * @return 四舍五入后的结果
     */
    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }
}