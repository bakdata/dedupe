package com.bakdata.deduplication.candidate_selection.online;

import com.bakdata.deduplication.candidate_selection.Candidate;

import java.util.List;

public interface OnlineCandidateSelection<T> {
    List<Candidate<T>> getCandidates(T newRecord);
}
