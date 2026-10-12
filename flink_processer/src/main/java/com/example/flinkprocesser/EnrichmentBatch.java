package com.example.flinkprocesser;

import java.util.List;

/** 一次窗口计算产生的互斥路由结果。 */
public record EnrichmentBatch(List<OutputEvent> success, List<OutputEvent> errors) {
    public EnrichmentBatch {
        // 防御性复制，避免窗口函数输出后结果集合又被调用方修改。
        success = List.copyOf(success);
        errors = List.copyOf(errors);
    }
}
