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

package com.bakdata.dedupe.matching;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Queue;
import lombok.Value;

/**
 * Implements a weakly stable matching based on the stable marriage with indifference (i.e., ties).
 * <p>"A matching will be called weakly stable unless there is a couple each of whom strictly prefers the other to
 * his/her partner in the matching. It is not hard to see that, in the stable marriage case, if ties are broken
 * arbitrarily, any matching that is stable in the resulting (strict) instance is weakly stable in the original
 * instance. So the Gale/Shapley algorithm (or other algorithms that find a stable matching in the classical case) may
 * be applied in this context to find a weakly stable matching."[1]</p>
 * <p>The preference list may be incomplete if some pairs absolutely do not want to matched. However, in this case,
 * there may be fewer matches than elements.</p>
 *
 * <p>[1] Irving, Robert W. (1994-02-15). "Stable marriage and indifference". Discrete Applied Mathematics. 48 (3):
 * 261–272. doi:10.1016/0166-218X(92)00179-P.</p>
 *
 * @param <T> the type of the record.
 */
@Value
public class WeaklyStableMarriage<T> extends AbstractStableMarriage<T> {
    @Override
    protected AbstractStableMarriage.Matcher createMatcher(final List<? extends Queue<List<Integer>>> mensFavoriteWomen,
            final List<? extends Queue<List<Integer>>> womensFavoriteMen) {
        return new Matcher(mensFavoriteWomen, womensFavoriteMen);
    }

    @VisibleForTesting
    static class Matcher extends AbstractStableMarriage.AbstractMatcher {
        Matcher(final List<? extends Queue<List<Integer>>> mensFavoriteWomen,
                final List<? extends Queue<List<Integer>>> womensFavoriteMen) {
            super(mensFavoriteWomen, womensFavoriteMen);
        }

        @Override
        protected void match() {
            // Assign each person to be free;
            freeMen.set(0, mensFavoriteWomen.size());
            // while (some man m is free) do
            while (!freeMen.isEmpty()) {
                final int m = freeMen.nextSetBit(0);
                final List<Integer> highestRankedWomen = mensFavoriteWomen.get(m).peek();
                if (highestRankedWomen == null) {
                    // no more women to propose to (poor guy)
                    freeMen.clear(m);
                } else {
                    // w := first woman on m’s list;
                    Integer w = highestRankedWomen.get(0);
                    // if (some man m' is engaged to w) then
                    Integer m_ = Iterables.getFirst(engagements.column(w).keySet(), null);
                    if (m_ != null) {
                        breakEngangement(m_, w);
                        freeMen.set(m_);
                    }
                    freeMen.clear(m);
                    propose(m, w);
                    //for each (successor m" of m on w’s list) do
                    getSuccessors(womensFavoriteMen.get(w), m).forEach(m__ -> {
                        delete(m__, w);
                    });
                }
            }
        }
    }
}
