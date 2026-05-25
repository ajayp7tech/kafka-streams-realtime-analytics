package com.ajayp.streams.service;
import com.ajayp.streams.model.AggregatedMetric;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import java.math.BigDecimal;
import static org.assertj.core.api.Assertions.*;
@ExtendWith(MockitoExtension.class)
class MetricsServiceTest {
    @InjectMocks
    private MetricsService metricsService;

    @BeforeEach
    void setUp() { metricsService.reset(); }

    @Test
    void recordOrder_shouldIncrementCount() {
        metricsService.recordOrder(new BigDecimal("50.00"));
        metricsService.recordOrder(new BigDecimal("75.00"));
        AggregatedMetric metrics = metricsService.getCurrentMetrics();
        assertThat(metrics.orderCount()).isEqualTo(2);
        assertThat(metrics.totalRevenue()).isEqualByComparingTo("125.00");
    }

    @Test
    void getCurrentMetrics_shouldCalculateAverage() {
        metricsService.recordOrder(new BigDecimal("100.00"));
        metricsService.recordOrder(new BigDecimal("50.00"));
        AggregatedMetric metrics = metricsService.getCurrentMetrics();
        assertThat(metrics.avgOrderValue()).isEqualByComparingTo("75.00");
    }

    @Test
    void reset_shouldClearAllMetrics() {
        metricsService.recordOrder(new BigDecimal("100.00"));
        metricsService.reset();
        AggregatedMetric metrics = metricsService.getCurrentMetrics();
        assertThat(metrics.orderCount()).isZero();
        assertThat(metrics.totalRevenue()).isEqualByComparingTo("0.00");
    }
}