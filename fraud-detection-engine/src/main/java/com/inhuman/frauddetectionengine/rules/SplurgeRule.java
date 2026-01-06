package com.inhuman.frauddetectionengine.rules;

import com.inhuman.frauddetectionengine.interfaces.FraudRule;
import com.inhuman.frauddetectionengine.models.TransactionProfile;
import com.inhuman.frauddetectionengine.models.ZScoreCalc;
import com.inhuman.frauddetectionengine.repos.SplurgeRepository;
import dtos.TransactionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "fraud.rules.splurge", name = "enabled", havingValue = "true")
public class SplurgeRule implements FraudRule {

    @Value("${fraud.rules.splurge.smoothing-factor}")
    private double alpha;
    @Value("${fraud.rules.splurge.score}")
    private int splurgeScore;
    @Value("${fraud.rules.splurge.variance.min}")
    private double varianceMin;
    @Value("${fraud.rules.splurge.cold-start-multiplier}")
    private double coldStartMultiplier;
    @Value("${fraud.rules.splurge.zscore.soft-threshold}")
    private double softThreshold;
    @Value("${fraud.rules.splurge.zscore.hard-threshold}")
    private double hardThreshold;
    @Value("${fraud.rules.splurge.zscore.score.threshold-cap}")
    private double thresholdCap;

    private final SplurgeRepository splurgeRepo;

    @Override
    public int evaluate(TransactionEvent e) {
        //get the user's transaction profile
        TransactionProfile profile = splurgeRepo.getProfile(e.getAccountId());

        //check if this is the first transaction
        if(profile == null) {
            splurgeRepo.updateProfile(
                    new TransactionProfile(e.getAmount(),
                            Math.pow(e.getAmount() * coldStartMultiplier, 2)), e.getAccountId()
            );
            return 0;
        }
        //calc the Z-Score
        ZScoreCalc info = calZScore(profile, e.getAmount());

        double zscore = info.getZScore();
        log.info("Z-Score: {}", zscore);

        //learn only if zscore is less than the hard threshold
        if(zscore < hardThreshold) {
            profile.setMean(info.getMean());
            profile.setVariance(info.getVariance());
            splurgeRepo.updateProfile(profile, e.getAccountId());
        }

        //return the relative splurge score only if the zscore > soft threshold
        if(zscore > softThreshold) {
            //calculate diff and return the score * diff
            return (int) Math.min(
                    thresholdCap,
                    splurgeScore * (zscore - softThreshold + 1)
            );
        }
        //else return 0
        return 0;
    }

    private ZScoreCalc calZScore(TransactionProfile profile, double x) {
        double meanPrev = profile.getMean();
        double varPrev  = Math.max(profile.getVariance(), varianceMin);

        //cal the zscore on the basis of prev info
        double z = (x - meanPrev) / Math.sqrt(varPrev);

        //cal the new info but don't update it yet
        double meanNext = (alpha * x) + ((1 - alpha) * meanPrev);
        double varianceNext = (alpha * Math.pow(x - meanNext, 2)) + ((1 - alpha) * varPrev);
        varianceNext = Math.max(varianceNext, varianceMin);

        return new ZScoreCalc(z, meanNext, varianceNext);
    }

}
