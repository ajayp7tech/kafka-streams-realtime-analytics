package com.ajayp.streams.controller;
import com.ajayp.streams.model.AggregatedMetric;
import com.ajayp.streams.service.MetricsService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
@RestController
@RequestMapping("/api/v1/metrics")
@RequiredArgsConstructor
public class MetricsController {
    private final MetricsService metricsService;
    @GetMapping("/orders")
    public ResponseEntity<AggregatedMetric> getMetrics() {
        return ResponseEntity.ok(metricsService.getCurrentMetrics());
    }
}