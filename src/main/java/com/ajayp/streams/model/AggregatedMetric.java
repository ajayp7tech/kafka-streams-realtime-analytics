package com.ajayp.streams.model;
import java.math.BigDecimal;
import java.time.LocalDateTime;
public record AggregatedMetric(
    long orderCount,
    BigDecimal totalRevenue,
    BigDecimal avgOrderValue,
    LocalDateTime windowStart,
    LocalDateTime windowEnd
) {}