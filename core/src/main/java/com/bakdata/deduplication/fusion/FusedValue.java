package com.bakdata.deduplication.fusion;

import com.bakdata.deduplication.clustering.Cluster;
import lombok.Value;

import java.util.List;

@Value
public class FusedValue<T> {
    T value;
    Cluster<T> originalValues;
    List<Exception> exceptions;
}
