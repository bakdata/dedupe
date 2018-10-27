package com.bakdata.deduplication.fusion;

import lombok.Value;

import java.time.LocalDateTime;

@Value
public class AnnotatedValue<T> {
    T value;
    Source source;
    LocalDateTime dateTime;

    public static <T> AnnotatedValue<T> calculated(T value) {
        return new AnnotatedValue<>(value, Source.getCalculated(), LocalDateTime.now());
    }

    @SuppressWarnings("unchecked")
    public <S> AnnotatedValue<S> withValue(S value) {
        return this.value == value ? (AnnotatedValue<S>) this : new AnnotatedValue<>(value, this.source, this.dateTime);
    }
}
