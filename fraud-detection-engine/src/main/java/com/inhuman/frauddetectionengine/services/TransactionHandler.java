package com.inhuman.frauddetectionengine.services;

import dtos.TransactionEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class TransactionHandler {

    @KafkaListener(topics = "Transactions", groupId = "transaction-handler")
    public void handle(TransactionEvent transactionEvent) {
        System.out.println("\n\n\n\n\n\n\n\n\n\n");
        if(detectFraud(transactionEvent)) {
            System.out.println("Fraud Detected: " + transactionEvent);
        }else{
            System.out.println("Transaction Approved: " + transactionEvent);
        }
        System.out.println("\n\n\n\n\n\n\n\n\n\n");
    }

    boolean detectFraud(TransactionEvent transactionEvent) {
        return true;
    }
}
