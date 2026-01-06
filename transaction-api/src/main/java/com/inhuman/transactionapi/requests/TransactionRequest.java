package com.inhuman.transactionapi.requests;

import lombok.Data;

@Data
public class TransactionRequest {
    private String transactionId;
    private String accountId;
    private Double amount;
}
