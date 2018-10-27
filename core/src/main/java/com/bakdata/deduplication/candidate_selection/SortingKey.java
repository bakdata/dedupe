package com.bakdata.deduplication.candidate_selection;

import lombok.Value;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Function;

@Value
public class SortingKey<T> {
    String name;
    Function<@NonNull T, @Nullable Comparable<?>> keyExtractor;
}
