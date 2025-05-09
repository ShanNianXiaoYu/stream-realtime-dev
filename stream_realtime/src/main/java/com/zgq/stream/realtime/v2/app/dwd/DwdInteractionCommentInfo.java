package com.zgq.stream.realtime.v2.app.dwd;

import com.zgq.stream.realtime.v1.constant.Constant;
import com.zgq.stream.realtime.v1.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.zgq.stream.realtime.v2.app.dwd.DwdInteractionCommentInfo
 * @Author guoqiang.zhang
 * @Date 2025/5/4 14:39
 * @description:
 */

public class DwdInteractionCommentInfo {
    public static void main(String[] args) throws Exception {
//       创建 flink 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//      设置并行度
        env.setParallelism(4);
//      创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//      设置检查点，每5秒检查一次，精准一次
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//       设置空闲状态的保留时间为 30 分钟 5 秒
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));

//        获取 kafka 主题topic_db数据，用于接受原始业务数据变更
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +        // 变更后的数据（Map结构）
                "  source MAP<string, string>, \n" +    // 数据源信息
                "  `op` string, \n" +                               // 操作类型（c=创建，u=更新，d=删除，r=读取）
                "  `ts_ms` bigint " +                              // 变更时间戳
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        tableEnv.executeSql("select * from topic_db").print();

//        从topic_db中筛选评论信息表(comment_info)的读取操作数据
        Table commentInfo = tableEnv.sqlQuery("select  \n" +
                "    `after`['id'] as id, \n" +
                "    `after`['user_id'] as user_id, \n" +
                "    `after`['sku_id'] as sku_id, \n" +
                "    `after`['appraise'] as appraise, \n" +
                "    `after`['comment_txt'] as comment_txt, \n" +
                "    `after`['create_time'] as create_time, " +
                "     ts_ms " +
                "     from topic_db where source['table'] = 'comment_info' and op = 'r'");
        commentInfo.execute().print();
//        创建评论信息表临时视图
        tableEnv.createTemporaryView("comment_info",commentInfo);

//        从HBase中读取字典表(dim_base_dic)
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic"));
        tableEnv.executeSql("select * from base_dic").print();

//         将评论信息与字典表进行关联，获取评价等级的名称
        Table joinedTable = tableEnv.sqlQuery("SELECT  \n" +
                "    id,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    appraise,\n" +
                "    dic.dic_name appraise_name,\n" +
                "    comment_txt,\n" +
                "    ts_ms \n" +
                "    FROM comment_info AS c\n" +
                "    JOIN base_dic AS dic\n" +
                "    ON c.appraise = dic.dic_code");
        joinedTable.execute().print();

        //创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE "+Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+" (\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    appraise string,\n" +
                "    appraise_name string,\n" +
                "    comment_txt string,\n" +
                "    ts_ms bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") " + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
//        写入 kafka 主题
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
//      执行flink
        env.execute("dwd_join");
    }
}
//         **topic_db**：这是一个来源于 Kafka 的表，主要用于接收原始业务数据变更信息。
//        **base_dic**：此表来自 HBase，属于字典表，在代码里起到的作用是将评论等级的编码转换为对应的名称。

//         1. **comment_info**：它是基于`topic_db`创建的临时视图，专门用于存放评论信息。
//        2. **joinedTable**：这是通过将`comment_info`和`base_dic`进行关联后得到的结果表。
//        3. **dwd_interaction_comment_info**：这是一个 Kafka 表，其用途是存储关联后的最终数据。


