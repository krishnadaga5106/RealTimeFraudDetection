package com.inhuman.frauddetectionengine.rules;

import com.inhuman.frauddetectionengine.interfaces.FraudRule;
import com.inhuman.frauddetectionengine.models.TransactionProfile;
import com.inhuman.frauddetectionengine.repos.SplurgeRepository;
import dtos.TransactionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "fraud.rules.splurge", name = "enabled", havingValue = "true")
public class SplurgeRule implements FraudRule {

    @Value("${fraud.rules.splurge.score}")
    private int splurgeScore;
    private final SplurgeRepository splurgeRepo;

    @Override
    public int evaluate(TransactionEvent e) {
        //get the user's transaction profile
        TransactionProfile profile = splurgeRepo.getProfile(e.getTransactionId());

        //check if this is the first transaction
        if(profile == null) {
            updateProfile(null, e.getAmount(), e.getAccountId());
            return 0;
        }
        //calc the Z-Score
        int zscore = Math.toIntExact(
                (e.getAmount() - updatedMean(profile, e.getAmount())) / updatedSD(profile, e.getAmount()));
        log.info("Z-Score: {}", zscore);

        if(zscore > 3)
            return splurgeScore;

        updateProfile(profile, e.getAmount(), e.getAccountId());
        return 0;
    }

    private void updateProfile(TransactionProfile profile, Long currAmount, String accountId) {
        if(profile == null) {
            profile = new TransactionProfile(0L, 0L, 0L);
        }
        profile.setCount(profile.getCount() + 1);
        profile.setSum(profile.getSum() + currAmount);
        profile.setSumSq(profile.getSumSq() + (long) Math.pow(currAmount, 2));

        splurgeRepo.updateProfile(profile, accountId);
    }

    private Long updatedMean(TransactionProfile profile, Long currAmount) {
        return (profile.getSum() + currAmount) / profile.getCount() + 1;
    }

    private Long updatedSD(TransactionProfile profile, Long currAmount) {
        return (long) Math.sqrt(
                profile.getSumSq() + Math.pow(currAmount, 2) / profile.getCount()
        );
    }

}
