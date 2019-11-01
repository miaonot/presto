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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

@Immutable
public final class GremlinNode
        extends PlanNode
{
    private final Optional<String> gremlin;
    private final TableHandle table;
    private final Map<VariableReferenceExpression, ColumnHandle> assignments;
    private final List<VariableReferenceExpression> outputVariables;

    @JsonCreator
    public GremlinNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("table") TableHandle table,
            @JsonProperty("gremlin") Optional<String> gremlin,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables,
            @JsonProperty("assignments") Map<VariableReferenceExpression, ColumnHandle> assignments)
    {
        super(id);
        this.gremlin = requireNonNull(gremlin, "gremlin is null");
        this.table = table;
        this.outputVariables = unmodifiableList(requireNonNull(outputVariables, "outputVariables is null"));
        this.assignments = assignments;
    }

    @JsonProperty("gremlin")
    public Optional<String> getGremlin()
    {
        return gremlin;
    }

    @JsonProperty("table")
    public TableHandle getTable()
    {
        return table;
    }

    @JsonProperty("assignments")
    public Map<VariableReferenceExpression, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return emptyList();
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return this;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitGremlin(this, context);
    }

    private static void checkArgument(boolean test, String errorMessage)
    {
        if (!test) {
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
