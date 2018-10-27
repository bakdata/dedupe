package com.bakdata.deduplication.candidate_selection;

import lombok.Value;

@Value
public class Candidate<T> {
    T newRecord;
    T oldRecord;
}
