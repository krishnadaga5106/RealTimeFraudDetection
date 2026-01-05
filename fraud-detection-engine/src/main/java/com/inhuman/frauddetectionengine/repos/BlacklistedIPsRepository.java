package com.inhuman.frauddetectionengine.repos;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class BlacklistedIPsRepository {

    @Value("${fraud.rules.blacklist.key}")
    private String key;

    private final RedisTemplate<String, Object> redisTemplate;

    public boolean has(String ip) {
        return redisTemplate.opsForSet().isMember(key, ip);
    }
}
