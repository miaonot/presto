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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Match
        extends Relation
{
    private final GraphPattern graphPattern;
    private final Optional<GraphFunc> graphFunc;
    private final Identifier relationName;
    private final Optional<List<Identifier>> columnAliases;

    public Match(GraphPattern graphPattern, Optional<GraphFunc> graphFunc, Identifier relationName, Optional<List<Identifier>> columnAliases)
    {
        this(Optional.empty(), graphPattern, graphFunc, relationName, columnAliases);
    }

    public Match(NodeLocation location, GraphPattern graphPattern, Optional<GraphFunc> graphFunc, Identifier relationName, Optional<List<Identifier>> columnAliases)
    {
        this(Optional.of(location), graphPattern, graphFunc, relationName, columnAliases);
    }

    public Match(Optional<NodeLocation> location, GraphPattern graphPattern, Optional<GraphFunc> graphFunc, Identifier relationName, Optional<List<Identifier>> columnAliases)
    {
        super(location);
        this.graphPattern = requireNonNull(graphPattern, "graphPattern is null");
        this.graphFunc = requireNonNull(graphFunc, "graphFunc is null");
        this.relationName = requireNonNull(relationName, "relationName is null");
        this.columnAliases = columnAliases;
    }

    public GraphPattern getGraphPattern()
    {
        return graphPattern;
    }

    public Optional<GraphFunc> getGraphFunc()
    {
        return graphFunc;
    }

    public Identifier getRelationName()
    {
        return relationName;
    }

    public Optional<List<Identifier>> getColumnAliases()
    {
        return columnAliases;
    }

    public List<String> getNodeTypes()
    {
        return graphPattern.getNodeTypes();
    }

    public List<String> getRelationshipTypes()
    {
        return graphPattern.getRelationshipTypes();
    }

    public List<String> getNodeNames()
    {
        return graphPattern.getNodeNames();
    }

    public List<String> getRelationshipNames()
    {
        return graphPattern.getRelationshipNames();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMatch(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(graphPattern)
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(graphPattern, graphFunc, relationName, columnAliases);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Match o = (Match) obj;
        return Objects.equals(graphPattern, o.graphPattern)
                && Objects.equals(graphFunc, o.graphFunc)
                && Objects.equals(relationName, o.relationName)
                && Objects.equals(columnAliases, o.columnAliases);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("graphPattern", graphPattern)
                .add("graphFunc", graphFunc)
                .add("relationName", relationName)
                .add("columnAliases", columnAliases)
                .toString();
    }
}
