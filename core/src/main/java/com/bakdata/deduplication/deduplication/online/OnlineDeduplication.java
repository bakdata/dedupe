package com.bakdata.deduplication.deduplication.online;

import java.util.List;

public interface OnlineDeduplication<T> {
    T deduplicate(T newRecord);
}
