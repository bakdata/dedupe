/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.bakdata.dedupe.person;

import com.bakdata.dedupe.clustering.Clustering;
import com.bakdata.dedupe.clustering.ConsistentClustering;
import com.bakdata.dedupe.clustering.ClusterIdGenerators;
import com.bakdata.dedupe.clustering.RefineCluster;
import com.bakdata.dedupe.clustering.RefinedTransitiveClosure;
import lombok.Value;
import lombok.experimental.Delegate;

@Value
public class PersonClustering implements Clustering<Long, Person> {
    RefineCluster<Long, Person> refineCluster = RefineCluster.<Long, Person>builder()
            .classifier(new PersonClassifier())
            .clusterIdGenerator(ClusterIdGenerators.longGenerator())
            .build();

    Clustering<Long, Person> refinedTransitiveClosure = RefinedTransitiveClosure.<Long, Person, String>builder()
            .refineCluster(this.refineCluster)
            .idExtractor(Person::getId)
            .build();

    @Delegate
    Clustering<Long, Person> clustering = ConsistentClustering.<Long, Person, String>builder()
            .clustering(this.refinedTransitiveClosure)
            .idExtractor(Person::getId)
            .build();
}
