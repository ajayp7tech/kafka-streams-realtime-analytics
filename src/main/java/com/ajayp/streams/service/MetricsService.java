package com.ajayp.streams.service;
import com.ajayp.streams.model.AggregatedMetric;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
@Service
@Slf4j
public class MetricsService {
    private final AtomicLong orderCount = new AtomicLong(0);
    private final AtomicReference<BigDecimal> totalRevenue = new AtomicReference<>(BigDecimal.ZERO);

    public void recordOrder(BigDecimal amount) {
        orderCount.incrementAndGet();
        totalRevenue.updateAndGet(current -> current.add(amount));
        log.info("Recorded order. Total count: {}, Total revenue: {}", orderCount.get(), totalRevenue.get());
    }

    public AggregatedMetric getCurrentMetrics() {
        long count = orderCount.get();
        BigDecimal revenue = totalRevenue.get();
        BigDecimal avg = count > 0 ? revenue.divide(BigDecimal.valueOf(count), 2, java.math.RoundingMode.HALF_UP) : BigDecimal.ZERO;
        return new AggregatedMetric(count, revenue, avg, LocalDateTime.now().minusMinutes(5), LocalDateTime.now());
    }

    public void reset() {
        orderCount.set(0);
        totalRevenue.set(BigDecimal.ZERO);
    }
}