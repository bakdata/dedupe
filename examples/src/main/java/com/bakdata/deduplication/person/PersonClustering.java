package com.bakdata.deduplication.person;

import com.bakdata.deduplication.clustering.Clustering;
import com.bakdata.deduplication.clustering.ConsistentClustering;
import com.bakdata.deduplication.clustering.RefineCluster;
import com.bakdata.deduplication.clustering.RefinedTransitiveClosure;
import lombok.Value;
import lombok.experimental.Delegate;

@Value
public class PersonClustering implements Clustering<Person> {
    RefineCluster<Person, String> refineCluster = RefineCluster.<Person, String>builder()
            .idExtractor(Person::getId)
            .classifier(new PersonClassifier())
            .build();

    Clustering<Person> refinedTransitiveClosure = RefinedTransitiveClosure.<Person, String>builder()
            .refineCluster(refineCluster)
            .build();

    @Delegate
    Clustering<Person> clustering = ConsistentClustering.<Person, String>builder()
            .clustering(refinedTransitiveClosure)
            .idExtractor(Person::getId)
            .build();
}
