package com.zgq.stream.realtime.v1.function;

import com.zgq.stream.realtime.v1.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Package com.zgq.stream.realtime.v1.function.KeywordUDTF
 * @Author guoqiang.zhang
 * @Date 2025/5/4 13:49
 * @description:
 */

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction < Row > {
    public void eval(String text) {
        for (String keyword : KeywordUtil.analyze(text)) {
            collect(Row.of(keyword));
        }
    }
}