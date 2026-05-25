package com.ajayp.streams.model;
import java.math.BigDecimal;
import java.time.LocalDateTime;
public record RawEvent(
    String eventId,
    String type,
    String customerId,
    BigDecimal amount,
    LocalDateTime timestamp
) {}