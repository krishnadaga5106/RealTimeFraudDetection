package com.inhuman.frauddetectionengine.interfaces;

import dtos.TransactionEvent;

public interface FraudRule {
    int evaluate(TransactionEvent transactionEvent);
}
