package com.bakdata.deduplication.classifier;

import com.bakdata.deduplication.candidate_selection.Candidate;

public interface Classifier<T> {
    Classification classify(Candidate<T> candidate);
}