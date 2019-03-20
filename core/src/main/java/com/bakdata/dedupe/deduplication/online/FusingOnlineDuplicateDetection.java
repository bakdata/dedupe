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
package com.bakdata.dedupe.deduplication.online;

import com.bakdata.dedupe.clustering.Cluster;
import com.bakdata.dedupe.clustering.Clusters;
import com.bakdata.dedupe.duplicate_detection.online.OnlineDuplicateDetection;
import com.bakdata.dedupe.fusion.FusedValue;
import com.bakdata.dedupe.fusion.Fusion;
import com.bakdata.dedupe.fusion.IncompleteFusionHandler;
import java.util.Iterator;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;


/**
 * A full online deduplication process, which
 * <ul>
 * <li>Retrieves duplicate clusters through {@link OnlineDeduplication}.</li>
 * <li>Fuses the duplicate clusters into reconciled records. </li>
 * </ul>
 */
@Value
@Builder
public class FusingOnlineDuplicateDetection<C extends Comparable<C>, T> implements OnlineDeduplication<T> {
    /**
     * The duplicate detection returning duplicate clusters.
     */
    @NonNull
    OnlineDuplicateDetection<C, T> duplicateDetection;
    /**
     * The fusion implementation that reconciles the clusters into new records.
     */
    @NonNull
    Fusion<T> fusion;
    /**
     * A callback for non-trivial clusters.
     */
    @Builder.Default
    @NonNull
    IncompleteFusionHandler<T> incompleteFusionHandler = IncompleteFusionHandler.dontFuse();

    @Override
    public @NonNull T deduplicate(final @NonNull T newRecord) {
        final Stream<Cluster<C, T>> clusters = this.duplicateDetection.detectDuplicates(newRecord);
        final Iterator<Cluster<C, T>> clusterIterator = clusters.iterator();

        if (!clusterIterator.hasNext()) {
            return newRecord;
        }

        final @NonNull FusedValue<T> fusedValue =
                this.fusion.fuse(Clusters.getContainingCluster(clusterIterator, newRecord));
        return this.incompleteFusionHandler.apply(fusedValue)
                .map(FusedValue::getValue)
                .orElse(newRecord);
    }
}
