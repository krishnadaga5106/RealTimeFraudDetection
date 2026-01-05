package com.inhuman.frauddetectionengine.services;

import com.inhuman.frauddetectionengine.interfaces.FraudRule;
import dtos.TransactionEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class RiskEngine {

    @Value("${fraud.global.block-threshold}")
    private int blockThreshold;

    private final List<FraudRule> activeRules;

    public int evaluateRisk(TransactionEvent transactionEvent) {
        int riskScore = 0;
        for(FraudRule rule : activeRules){
            //no further need to check more rules
            //if the score is already greater than the block threshold
            if(riskScore > blockThreshold) return riskScore;
            riskScore += rule.evaluate(transactionEvent);
        }
        return riskScore;
    }
}


