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
package com.bakdata.dedupe.fusion;

import com.bakdata.dedupe.clustering.Cluster;
import java.util.Optional;
import lombok.NonNull;

/**
 * Fuses a cluster of duplicates to one representation.
 * <p>Fusion may be incomplete in which case {@link FusedValue} signals possible issues. The fusion should work
 * fail-late: It should try to resolve as many information as possible and collect all issues on the way.</p>
 * <p>There is no distinction between fusion in an online and offline process.</p>
 *
 * @param <T> the type of the record
 */
@FunctionalInterface
public interface Fusion<T> {
    /**
     * Fuses a cluster of duplicates to one representation.
     *
     * @param cluster a cluster of duplicates.
     * @return a fused value.
     */
    @NonNull FusedValue<T> fuse(@NonNull Cluster<?, T> cluster);

    /**
     * Returns the fused value for a cluster of duplicates. If fusion is not successful, a second try with the {@link
     * IncompleteFusionHandler} is performed. If that try fails as well, {@link Optional#empty()} is returned.
     *
     * @param cluster a cluster of duplicates.
     * @param incompleteFusionHandler tries to complete an incomplete fusion.
     * @return a complete fusion value or {@link Optional#empty()}.
     */
    default @NonNull Optional<T> fusedValue(@NonNull Cluster<?, T> cluster,
            @NonNull IncompleteFusionHandler<T> incompleteFusionHandler) {
        return Optional.of(this.fuse(cluster))
                .flatMap(incompleteFusionHandler::handlePartiallyFusedValue)
                .map(FusedValue::getValue);
    }
}
