package com.bakdata.deduplication.candidate_selection.offline;

import com.bakdata.deduplication.candidate_selection.Candidate;

import java.util.List;

public interface OfflineCandidateSelection<T> {
    List<Candidate<T>> getCandidates(List<T> newRecord);
}
