package com.zgq.stream.realtime.v1.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.zgq.stream.realtime.v1.bean.DimBaseCategory
 * @Author guoqiang.zhang
 * @Date 2025/5/14 19:16
 * @description:
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimBaseCategory implements Serializable {

    private String id;
    private String b3name;
    private String b2name;
    private String b1name;


}