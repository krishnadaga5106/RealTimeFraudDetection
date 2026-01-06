package com.inhuman.frauddetectionengine.repos;

import com.inhuman.frauddetectionengine.models.TransactionProfile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@Slf4j
@Repository
@RequiredArgsConstructor
public class SplurgeRepository {

    @Value("${fraud.rules.splurge.key}")
    private String key;

    private final RedisTemplate<String, Object> redisTemplate;

    public TransactionProfile getProfile(String accountId){
        List<Object> stats = redisTemplate.opsForHash()
                .multiGet(key + accountId, Arrays.asList("mean", "variance"));

        //check if new user
        if(stats.get(0) == null)
            return null;

        return new TransactionProfile(
                Double.parseDouble(stats.get(0).toString()),
                Double.parseDouble(stats.get(1).toString())
        );
    }

    public void updateProfile(TransactionProfile profile, String accountId) {
        log.info("updatedProfile: {}", profile);
        HashMap<String, Object> map = new HashMap<>();
        map.put("mean", profile.getMean());
        map.put("variance", profile.getVariance());

        redisTemplate.opsForHash().putAll(key + accountId, map);
    }
}
