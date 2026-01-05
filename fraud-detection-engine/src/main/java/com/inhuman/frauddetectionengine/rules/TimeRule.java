package com.inhuman.frauddetectionengine.rules;

import com.inhuman.frauddetectionengine.interfaces.FraudRule;
import dtos.TransactionEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "fraud.rules.time", name = "enabled", havingValue = "true")
public class TimeRule implements FraudRule {

    @Value("${fraud.rules.time.score}")
    private int timeScore;
    @Value("${fraud.rules.time.zone-id}")
    private String zoneId;
    @Value("${fraud.rules.time.start.time}")
    private int startTime;
    @Value("${fraud.rules.time.end.time}")
    private int endTime;

    @Override
    public int evaluate(TransactionEvent e) {
        int hour = timeHour(e.getTimestamp());
        log.info("Hour {}", hour);
        if(hour > startTime && hour < endTime)
            return timeScore;
        return 0;
    }

    private int timeHour(Long timestamp) {
        return Instant.ofEpochMilli(timestamp)
                .atZone(ZoneId.of(zoneId))
                .getHour();
    }
}
