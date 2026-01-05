package com.inhuman.frauddetectionengine.repos;

import dtos.TransactionEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class VelocityRepository {

    @Value("${fraud.rules.velocity.key}")
    private String velocityKey;
    @Value("${fraud.rules.velocity.transaction-limit}")
    private int transactionLimit;
    @Value("${fraud.rules.velocity.window.duration}")
    private Long windowLength;

    private final RedisTemplate<String, Object> redisTemplate;

    public int getTransactions(TransactionEvent e) {
        //add the current transaction to the z set
        redisTemplate.opsForZSet().add(
                velocityKey + e.getAccountId(),
                e.getTransactionId(),
                System.currentTimeMillis());

        //remove all the transactions before the start of the current window
        redisTemplate.opsForZSet().removeRangeByScore(velocityKey + e.getAccountId(), 0, System.currentTimeMillis() -  windowLength);

        //check if the size exceeded the transaction limit + 1
        int size = Math.toIntExact(redisTemplate.opsForZSet().zCard(velocityKey + e.getAccountId()));

        //(+1) to make sure that 1 more transaction stays in memory to fulfill the rule
        if(size > transactionLimit + 1){
            //(-1) here to remove 1 less transaction than the transaction limit
            redisTemplate.opsForZSet().removeRange(velocityKey + e.getAccountId(), 0, size - transactionLimit - 1);
            //update the new size
            size -= transactionLimit;
        }
        return size;
    }
}
