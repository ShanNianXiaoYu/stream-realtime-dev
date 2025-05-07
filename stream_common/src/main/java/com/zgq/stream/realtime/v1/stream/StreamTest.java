package com.zgq.stream.realtime.v1.stream;

import lombok.SneakyThrows;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.nio.charset.StandardCharsets;

/**
 * @Package com.stream.StreamTest
 * @Author zhou.han
 * @Date 2024/10/11 14:28
 * @description: Test
 */
public class StreamTest {
    @SneakyThrows
    public static void main(String[] args) {

        System.err.println(MD5Hash.getMD5AsHex("15".getBytes(StandardCharsets.UTF_8)));



    }
}
