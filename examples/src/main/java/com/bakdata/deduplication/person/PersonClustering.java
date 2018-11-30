package com.bakdata.deduplication.person;

import com.bakdata.deduplication.clustering.*;
import lombok.Value;
import lombok.experimental.Delegate;

@Value
public class PersonClustering implements Clustering<Long, Person> {
    RefineCluster<Long, Person, String> refineCluster = RefineCluster.<Long, Person, String>builder()
            .classifier(new PersonClassifier())
            .clusterIdGenerator(Cluster.longGenerator())
            .build();

    Clustering<Long, Person> refinedTransitiveClosure = RefinedTransitiveClosure.<Long, Person, String>builder()
            .refineCluster(refineCluster)
            .idExtractor(Person::getId)
            .build();

    @Delegate
    Clustering<Long, Person> clustering = ConsistentClustering.<Long, Person, String>builder()
            .clustering(refinedTransitiveClosure)
            .idExtractor(Person::getId)
            .build();
}
