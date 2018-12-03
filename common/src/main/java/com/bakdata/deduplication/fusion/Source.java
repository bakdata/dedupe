package com.bakdata.deduplication.fusion;

import lombok.Getter;
import lombok.Value;

@SuppressWarnings("WeakerAccess")
@Value
public class Source {
    String name;
    float weight;

    @Getter
    private static final Source Calculated = new Source("calculated", Float.NaN);
}
