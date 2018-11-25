package com.bakdata.deduplication.classifier;

import com.bakdata.deduplication.candidate_selection.Candidate;
import lombok.Value;

@Value
public class ClassifiedCandidate<T> {
    Candidate<T> candidate;
    Classification classification;
}