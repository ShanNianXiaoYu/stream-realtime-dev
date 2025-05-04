package com.zgq.stream.realtime.v1.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.zgq.stream.realtime.v1.bean.TrafficUvCt
 * @Author  guoqiang.zhang
 * @Date  2025/5/4 13:44
 * @description: 
*/

@Data
@AllArgsConstructor
public class TrafficUvCt {
 // 渠道
 String ch;
 // 独立访客数
 Integer uvCt;
}