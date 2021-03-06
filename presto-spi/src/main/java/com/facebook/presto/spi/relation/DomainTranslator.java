/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi.relation;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.predicate.TupleDomain;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface DomainTranslator
{
    interface ColumnExtractor<T>
    {
        Optional<T> extract(RowExpression expression);
    }

    RowExpression toPredicate(TupleDomain<VariableReferenceExpression> tupleDomain);

    /**
     * Convert a RowExpression predicate into an ExtractionResult consisting of:
     * 1) A successfully extracted TupleDomain
     * 2) An RowExpression fragment which represents the part of the original RowExpression that will need to be re-evaluated
     * after filtering with the TupleDomain.
     */
    <T> ExtractionResult<T> fromPredicate(ConnectorSession session, RowExpression predicate, ColumnExtractor<T> columnExtractor);

    class ExtractionResult<T>
    {
        private final TupleDomain<T> tupleDomain;
        private final RowExpression remainingExpression;

        public ExtractionResult(TupleDomain<T> tupleDomain, RowExpression remainingExpression)
        {
            this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
            this.remainingExpression = requireNonNull(remainingExpression, "remainingExpression is null");
        }

        public TupleDomain<T> getTupleDomain()
        {
            return tupleDomain;
        }

        public RowExpression getRemainingExpression()
        {
            return remainingExpression;
        }
    }

    ColumnExtractor<VariableReferenceExpression> BASIC_COLUMN_EXTRACTOR = expression -> {
        if (expression instanceof VariableReferenceExpression) {
            return Optional.of((VariableReferenceExpression) expression);
        }
        return Optional.empty();
    };
}
