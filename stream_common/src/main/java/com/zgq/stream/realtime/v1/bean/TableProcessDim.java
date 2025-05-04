package com.zgq.stream.realtime.v1.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.zgq.stream.realtime.v1.bean.TableProcessDim
 * @Author  guoqiang.zhang
 * @Date  2025/5/4 13:39
 * @description: 
*/

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableProcessDim {

 String sourceTable;

 String sinkTable;

 String sinkColumns;

 String sinkFamily;

 String sinkRowKey;

 String op;

}