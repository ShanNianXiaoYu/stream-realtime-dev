package com.zgq.test.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zgq.stream.realtime.v1.base2.DimCategoryCompare;
import com.zgq.stream.realtime.v1.bean.DimBaseCategory;
import com.zgq.stream.realtime.v1.utils.FlinkSourceUtil;
import com.zgq.stream.realtime.v1.utils.JdbcUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Package com.zgq.app.dwd.DwdScoreApp
 * @Author guoqiang.zhang
 * @Date 2025/5/14 14:46
 * @description: 用户行为评分计算作业（基于设备、搜索词等维度）
 */

public class DwdScore {

    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);

        // 从Kafka主题"minutes_page_Log"读取页面日志数据，消费者组为"page_Log"
        KafkaSource<String> kafkaSourceLog = FlinkSourceUtil.getKafkaSource("minutes_page_Log", "page_Log");
        // 读取Kafka数据，不使用水印
        SingleOutputStreamOperator<String> kafka_source_log = env.fromSource(kafkaSourceLog, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 将日志字符串解析为JSONObject
        SingleOutputStreamOperator<JSONObject> streamOperatorlog = kafka_source_log.map(JSON::parseObject);

        // 处理日志数据，计算用户行为评分维度
        SingleOutputStreamOperator<JSONObject> operator = streamOperatorlog.map(new RichMapFunction<JSONObject, JSONObject>() {

            private List<DimBaseCategory> dim_base_categories; // 三级分类基础信息列表
            private Map<String, DimBaseCategory> categoryMap; // 分类名称到分类信息的映射
            private List<DimCategoryCompare> dimCategoryCompares; // 分类对比字典列表
            private Connection connection; // 数据库连接

            // 各维度评分权重
            final double deviceRate = 0.1; // 设备维度权重
            final double searchRate = 0.15; // 搜索词维度权重
            final double timeRate = 0.1; // 时间维度权重

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化分类映射表
                categoryMap = new HashMap<>();
                // 获取数据库连接
                connection = JdbcUtil.getMySQLConnection();

                // 查询三级分类基础信息（关联一、二级分类）
                String sql1 = "select b3.id,                           \n" +
                        "            b3.name as b3name,                \n" +
                        "            b2.name as b2name,                \n" +
                        "            b1.name as b1name                 \n" +
                        "      from realtime_v1.base_category3 as b3  \n" +
                        "      join realtime_v1.base_category2 as b2  \n" +
                        "      on b3.category2_id = b2.id              \n" +
                        "      join realtime_v1.base_category1 as b1  \n" +
                        "      on b2.category1_id = b1.id                ";
                dim_base_categories = JdbcUtil.queryList(connection, sql1, DimBaseCategory.class, false);

                // 查询分类对比字典（搜索词对应的分类标签）
                String sql2 = "select id, category_name, search_category from realtime_v1.category_compare_dic;";
                dimCategoryCompares = JdbcUtil.queryList(connection, sql2, DimCategoryCompare.class, false);

                // 填充分类映射表
                for (DimBaseCategory category : dim_base_categories) {
                    categoryMap.put(category.getB3name(), category); // 存储三级分类名称对应的完整分类信息
                    System.err.println(category); // 打印分类信息
                }

                super.open(parameters);
            }

            @Override
            public JSONObject map(JSONObject jsonObject) {
                // 解析设备操作系统（取第一个元素，如"Android,11" → "Android"）
                String os = jsonObject.getString("os");
                String[] labels = os.split(",");
                String judge_os = labels[0];
                jsonObject.put("judge_os", judge_os); // 存储判断后的操作系统类型

                // 根据操作系统类型分配设备维度评分（不同年龄段的评分权重）
                if (judge_os.equals("iOS")) {
                    jsonObject.put("device_18_24", round(0.7 * deviceRate)); // 18-24岁评分
                    jsonObject.put("device_25_29", round(0.6 * deviceRate)); // 25-29岁评分
                    jsonObject.put("device_30_34", round(0.5 * deviceRate)); // 30-34岁评分
                    jsonObject.put("device_35_39", round(0.4 * deviceRate)); // 35-39岁评分
                    jsonObject.put("device_40_49", round(0.3 * deviceRate)); // 40-49岁评分
                    jsonObject.put("device_50", round(0.2 * deviceRate)); // 50岁以上评分
                } else if (judge_os.equals("Android")) {
                    jsonObject.put("device_18_24", round(0.8 * deviceRate)); // Android设备对应年龄段评分（高于iOS）
                    jsonObject.put("device_25_29", round(0.7 * deviceRate));
                    jsonObject.put("device_30_34", round(0.6 * deviceRate));
                    jsonObject.put("device_35_39", round(0.5 * deviceRate));
                    jsonObject.put("device_40_49", round(0.4 * deviceRate));
                    jsonObject.put("device_50", round(0.3 * deviceRate));
                }

                // 处理搜索词：匹配分类信息
                String searchItem = jsonObject.getString("search_item");
                if (searchItem != null && !searchItem.isEmpty()) {
                    DimBaseCategory category = categoryMap.get(searchItem); // 根据搜索词查找三级分类
                    if (category != null) {
                        jsonObject.put("b1_category", category.getB1name()); // 存储一级分类名称
                    }
                }

                // 处理分类标签：映射到搜索类别（如"时尚与潮流"、"性价比"等）
                String b1Category = jsonObject.getString("b1_category");
                if (b1Category != null && !b1Category.isEmpty()) {
                    for (DimCategoryCompare dimCategoryCompare : dimCategoryCompares) {
                        if (b1Category.equals(dimCategoryCompare.getCategoryName())) {
                            jsonObject.put("searchCategory", dimCategoryCompare.getSearchCategory()); // 存储搜索类别标签
                            break;
                        }
                    }
                }

                // 处理默认搜索类别
                String searchCategory = jsonObject.getString("searchCategory");
                if (searchCategory == null) {
                    searchCategory = "unknown"; // 未知分类默认值
                }

                // 根据搜索类别分配搜索维度评分（不同年龄段的评分权重）
                switch (searchCategory) {
                    case "时尚与潮流":
                        // 年轻群体评分更高
                        jsonObject.put("search_18_24", round(0.9 * searchRate));
                        jsonObject.put("search_25_29", round(0.7 * searchRate));
                        jsonObject.put("search_30_34", round(0.5 * searchRate));
                        jsonObject.put("search_35_39", round(0.3 * searchRate));
                        jsonObject.put("search_40_49", round(0.2 * searchRate));
                        jsonObject.put("search_50", round(0.1 * searchRate));
                        break;
                    case "性价比":
                        // 中年群体评分更高
                        jsonObject.put("search_18_24", round(0.2 * searchRate));
                        jsonObject.put("search_25_29", round(0.4 * searchRate));
                        jsonObject.put("search_30_34", round(0.6 * searchRate));
                        jsonObject.put("search_35_39", round(0.7 * searchRate));
                        jsonObject.put("search_40_49", round(0.8 * searchRate));
                        jsonObject.put("search_50", round(0.8 * searchRate));
                        break;
                    case "健康与养生":
                        // 中老年群体评分更高
                        jsonObject.put("search_18_24", round(0.1 * searchRate));
                        jsonObject.put("search_25_29", round(0.2 * searchRate));
                        jsonObject.put("search_30_34", round(0.4 * searchRate));
                        jsonObject.put("search_35_39", round(0.6 * searchRate));
                        jsonObject.put("search_40_49", round(0.8 * searchRate));
                        jsonObject.put("search_50", round(0.9 * searchRate));
                        break;
                    case "家庭与育儿":
                        // 30岁以上已婚群体评分更高
                        jsonObject.put("search_18_24", round(0.1 * searchRate));
                        jsonObject.put("search_25_29", round(0.2 * searchRate));
                        jsonObject.put("search_30_34", round(0.4 * searchRate));
                        jsonObject.put("search_35_39", round(0.6 * searchRate));
                        jsonObject.put("search_40_49", round(0.8 * searchRate));
                        jsonObject.put("search_50", round(0.7 * searchRate));
                        break;
                    case "科技与数码":
                        // 年轻群体评分更高
                        jsonObject.put("search_18_24", round(0.8 * searchRate));
                        jsonObject.put("search_25_29", round(0.6 * searchRate));
                        jsonObject.put("search_30_34", round(0.4 * searchRate));
                        jsonObject.put("search_35_39", round(0.3 * searchRate));
                        jsonObject.put("search_40_49", round(0.2 * searchRate));
                        jsonObject.put("search_50", round(0.1 * searchRate));
                        break;
                    case "学习与发展":
                        // 25-40岁群体评分更高
                        jsonObject.put("search_18_24", round(0.4 * searchRate));
                        jsonObject.put("search_25_29", round(0.5 * searchRate));
                        jsonObject.put("search_30_34", round(0.6 * searchRate));
                        jsonObject.put("search_35_39", round(0.7 * searchRate));
                        jsonObject.put("search_40_49", round(0.8 * searchRate));
                        jsonObject.put("search_50", round(0.7 * searchRate));
                        break;
                    default:
                        // 未知类别默认评分为0
                        jsonObject.put("search_18_24", 0);
                        jsonObject.put("search_25_29", 0);
                        jsonObject.put("search_30_34", 0);
                        jsonObject.put("search_35_39", 0);
                        jsonObject.put("search_40_49", 0);
                        jsonObject.put("search_50", 0);
                }

                return jsonObject;
            }

            // 数值四舍五入处理（保留三位小数）
            private double round(double value) {
                return BigDecimal.valueOf(value)
                        .setScale(3, RoundingMode.HALF_UP)
                        .doubleValue();
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        operator.print(); // 打印处理后的日志数据（包含评分维度）

        env.execute("minutes_page_Log"); // 执行Flink作业
    }
}