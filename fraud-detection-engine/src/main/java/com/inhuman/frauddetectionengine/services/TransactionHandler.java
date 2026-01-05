package com.inhuman.frauddetectionengine.services;

import dtos.TransactionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionHandler {

    @Value("${fraud.global.review-threshold}")
    private int reviewThreshold;

    @Value("${fraud.global.block-threshold}")
    private int blockThreshold;

    private final RiskEngine riskEngine;


    @KafkaListener(topics = "Transactions", groupId = "transaction-handler")
    public void handle(TransactionEvent transactionEvent) {
        int riskScore = riskEngine.evaluateRisk(transactionEvent);
        takeAction(transactionEvent, riskScore);
    }

    private void takeAction(TransactionEvent transactionEvent, int riskScore) {
        if(riskScore >= reviewThreshold)
            log.info("[REVIEW]: risk: {}, transaction: {}", riskScore,  transactionEvent);
        else if(riskScore >= blockThreshold)
            log.info("[BLOCK]: risk: {}, transaction: {}", riskScore, transactionEvent);
        else
            log.info("[APPROVED]: risk: {}, transaction: {}", riskScore, transactionEvent);
    }

}
