package com.inhuman.frauddetectionengine.repos;

import com.inhuman.frauddetectionengine.models.TransactionProfile;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class SplurgeRepository {

    @Value("${fraud.rules.splurge.key}")
    private String key;

    private final RedisTemplate<String, Object> redisTemplate;

    public TransactionProfile getProfile(String accountId){
        List<Object> stats = redisTemplate.opsForHash()
                .multiGet(key + accountId, Arrays.asList("sum", "sumSq", "count"));

        //check if new user
        if(stats.get(0) == null)
            return null;

        return new TransactionProfile(
                Long.parseLong(stats.get(0).toString()),
                Long.parseLong(stats.get(1).toString()),
                Long.parseLong(stats.get(2).toString())
        );
    }

    public void updateProfile(TransactionProfile profile, String accountId) {
        System.out.println("updatedProfile: " + profile);
        HashMap<String, Object> map = new HashMap<>();
        map.put("sum", profile.getSum());
        map.put("sumSq", profile.getSumSq());
        map.put("count", profile.getCount());

        redisTemplate.opsForHash().putAll(key + accountId, map);
    }
}
