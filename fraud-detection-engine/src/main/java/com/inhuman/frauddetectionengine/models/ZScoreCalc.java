package com.inhuman.frauddetectionengine.models;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ZScoreCalc {
    Double zScore;
    Double mean;
    Double variance;
}
