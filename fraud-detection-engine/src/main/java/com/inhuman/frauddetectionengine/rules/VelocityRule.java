package com.inhuman.frauddetectionengine.rules;

import com.inhuman.frauddetectionengine.interfaces.FraudRule;
import com.inhuman.frauddetectionengine.repos.VelocityRepository;
import dtos.TransactionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "fraud.rules.velocity", name = "enabled", havingValue = "true")
public class VelocityRule implements FraudRule {

    @Value("${fraud.rules.velocity.score}")
    private int velocityScore;

    @Value("${fraud.rules.velocity.transaction-limit}")
    private int transactionLimit;

    private final VelocityRepository velocityRepo;

    @Override
    public int evaluate(TransactionEvent e) {
        int trans = velocityRepo.getTransactions(e);
        log.info("Transactions: {}", trans);
        if(velocityRepo.getTransactions(e) > transactionLimit)
            return velocityScore;
        return 0;
    }
}
