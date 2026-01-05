package com.inhuman.frauddetectionengine.rules;

import com.inhuman.frauddetectionengine.interfaces.FraudRule;
import com.inhuman.frauddetectionengine.repos.BlacklistedIPsRepository;
import dtos.TransactionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "fraud.rules.blacklist", name = "enabled", havingValue = "true")
public class BlacklistRule implements FraudRule {

    @Value("${fraud.rules.blacklist.score}")
    private int blacklistThreshold;

    private final BlacklistedIPsRepository blacklistedIPsRepo;

    @Override
    public int evaluate(TransactionEvent transactionEvent) {
        log.info("checked for blacklist");
        if(blacklistedIPsRepo.has(transactionEvent.getIpAddress()))
            return blacklistThreshold;
        return 0;
    }
}
