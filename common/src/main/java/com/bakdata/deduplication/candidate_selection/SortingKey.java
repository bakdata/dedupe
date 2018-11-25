package com.bakdata.deduplication.candidate_selection;

import lombok.NonNull;
import lombok.Value;

import java.util.function.Function;

@Value
public class SortingKey<T> {
    @NonNull
    String name;
    @NonNull
    Function<T, Comparable<?>> keyExtractor;
}
