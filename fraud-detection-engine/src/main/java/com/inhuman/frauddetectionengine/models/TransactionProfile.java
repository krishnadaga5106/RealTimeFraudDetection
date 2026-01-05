package com.inhuman.frauddetectionengine.models;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TransactionProfile {
    private Long sum;
    private Long sumSq;
    private Long count;
}
