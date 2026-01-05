package com.inhuman.frauddetectionengine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;

@SpringBootApplication
@EnableRedisRepositories
public class FraudDetectionEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(FraudDetectionEngineApplication.class, args);
    }

}
