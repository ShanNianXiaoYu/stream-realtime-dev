package com.zgq.stream.realtime.v1.stream;

import com.stream.common.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.stream.Test
 * @Author zhou.han
 * @Date 2024/12/29 22:45
 * @description:
 */
public class Test {
    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("cdh03", 15455);

        dataStreamSource.print();




        env.execute();
    }

}
