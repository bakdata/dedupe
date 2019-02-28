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
package com.bakdata.deduplication.deduplication.online;

import com.bakdata.deduplication.clustering.Cluster;
import com.bakdata.deduplication.deduplication.HardFusionHandler;
import com.bakdata.deduplication.duplicate_detection.online.OnlineDuplicateDetection;
import com.bakdata.deduplication.fusion.FusedValue;
import com.bakdata.deduplication.fusion.Fusion;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class OnlinePairBasedDeduplication<T> implements OnlineDeduplication<T> {
    OnlineDuplicateDetection<?, T> duplicateDetection;
    Fusion<T> fusion;
    @Builder.Default
    HardFusionHandler<T> hardFusionHandler = HardFusionHandler.dontFuse();

    @Override
    public T deduplicate(final T newRecord) {
        final List<? extends Cluster<?, T>> clusters = this.duplicateDetection.detectDuplicates(newRecord);
        if (clusters.isEmpty()) {
            return newRecord;
        }

        final List<? extends Cluster<?, T>> mainClusters =
                clusters.stream().filter(c -> c.contains(newRecord)).collect(Collectors.toList());
        if (mainClusters.size() != 1) {
            throw new IllegalStateException(
                    "Expected exactly one cluster with the new record, but received " + clusters);
        }

        return Optional.of(this.fusion.fuse(mainClusters.get(0)))
                .flatMap(this.hardFusionHandler::handlePartiallyFusedValue)
                .map(FusedValue::getValue)
                .orElse(newRecord);
    }
}
