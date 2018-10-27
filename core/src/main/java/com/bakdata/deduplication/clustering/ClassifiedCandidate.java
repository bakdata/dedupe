package com.bakdata.deduplication.clustering;

import com.bakdata.deduplication.candidate_selection.Candidate;
import com.bakdata.deduplication.classifier.Classification;
import lombok.Value;

@Value
public class ClassifiedCandidate<T> {
    Candidate<T> candidate;
    Classification classification;
}