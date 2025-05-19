package com.zgq.stream.realtime.v1.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.zgq.stream.realtime.v1.bean.DimSkuInfoMsg
 * @Author guoqiang.zhang
 * @Date 2025/5/15 19:46
 * @description:
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimSkuInfoMsg {
    private String id;
    private String spuid;
    private String category3_id;
    private String tm_name;
}
