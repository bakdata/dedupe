package com.bakdata.deduplication.deduplication.online;

public interface OnlineDeduplication<T> {
    T deduplicate(T newRecord);
}
