package com.zgq.stream.realtime.v2.app.dwd;

import com.zgq.stream.realtime.v1.constant.Constant;
import com.zgq.stream.realtime.v1.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.zgq.stream.realtime.v2.app.dwd.DwdTradeCartAdd
 * @Author guoqiang.zhang
 * @Date 2025/5/4 14:39
 * @description:
 */

public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

//        获取 kafka  topic_db主题的数据
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  ts_ms bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        tableEnv.executeSql("select * from topic_db").print();

//       创建 base_dic 的表，存储字典信息
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
        );
        tableEnv.executeSql("select * from base_dic").print();

//        从 topic_db 表中查询购物车相关信息
        Table cartInfo = tableEnv.sqlQuery("select \n" +
                "   `after`['id'] id,\n" +
                "   `after`['user_id'] user_id,\n" +
                "   `after`['sku_id'] sku_id,\n" +
                "   if(op = 'r',`after`['sku_num'], CAST((CAST(after['sku_num'] AS INT) - CAST(`after`['sku_num'] AS INT)) AS STRING)) sku_num,\n" +
                "   ts_ms \n" +
                "   from topic_db \n" +
                "   where source['table'] = 'cart_info' \n" +
                "   and ( op = 'r' or \n" +
                "   ( op='r' and after['sku_num'] is not null and (CAST(after['sku_num'] AS INT) > CAST(after['sku_num'] AS INT))))"
        );
        cartInfo.execute().print();

//        创建dwd_trade_cart_add表
        tableEnv.executeSql(" create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts_ms bigint, \n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                " )" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
//        写入
        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
//        执行 Flink
        env.execute("dwd_kafka");
    }
}

//         1. **cartInfo**：·由`topic_db`查询生成的`Table`对象，提取`id`、`user_id`等字段数据。
//        2. **dwd_trade_cart_add**：基于 Kafka 的表，结构含`id`、`user_id`等，用于存储处理后的购物车数据，将`cartInfo`数据插入其中。
