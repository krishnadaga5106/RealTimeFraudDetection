package com.inhuman.transactionapi.contollers;

import com.inhuman.transactionapi.requests.TransactionRequest;
import dtos.TransactionEvent;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequiredArgsConstructor
@RequestMapping("/tx")
public class TransactionController {

    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    @PostMapping("/")
    public ResponseEntity<String> newTransaction(@RequestBody TransactionRequest txReq, HttpServletRequest request) {
        TransactionEvent event = new TransactionEvent(
                txReq.getTransactionId(),
                txReq.getAccountId(),
                txReq.getAmount(),
                System.currentTimeMillis(),
                request.getRemoteAddr()
        );

        kafkaTemplate.send("Transactions", txReq.getAccountId(), event);

        return new ResponseEntity<>("Transaction created", HttpStatus.ACCEPTED);
    }
}
