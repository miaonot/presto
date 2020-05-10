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

public class RelationshipDetail
        extends Node
{
    private final Optional<Identifier> relationName;
    private final Optional<LabelNames> labelNames;
    private final Optional<RelationshipRange> range;

    public RelationshipDetail(
            Optional<Identifier> relationName,
            Optional<LabelNames> labelNames,
            Optional<RelationshipRange> range)
    {
        this(Optional.empty(), relationName, labelNames, range);
    }

    public RelationshipDetail(
            NodeLocation location,
            Optional<Identifier> relationName,
            Optional<LabelNames> labelNames,
            Optional<RelationshipRange> range)
    {
        this(Optional.of(location), relationName, labelNames, range);
    }

    public RelationshipDetail(
            Optional<NodeLocation> location,
            Optional<Identifier> relationName,
            Optional<LabelNames> labelNames,
            Optional<RelationshipRange> range)
    {
        super(location);
        this.relationName = relationName;
        this.labelNames = labelNames;
        this.range = range;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRelationshipDetail(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        relationName.ifPresent(nodes::add);
        labelNames.ifPresent(nodes::add);
        range.ifPresent(nodes::add);
        return nodes.build();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RelationshipDetail o = (RelationshipDetail) obj;
        return relationName.equals(o.relationName) &&
                labelNames.equals(o.labelNames) &&
                range.equals(o.range);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relationName, labelNames, range);
    }

    @Override
    public String toString()
    {
        return relationName.map(identifier -> labelNames.map(names -> range.map(range -> identifier.getValue() + names.toString() + range.toString())
                .orElseGet(() -> identifier.getValue() + names.toString()))
                .orElseGet(() -> range.map(range -> identifier.getValue() + range.toString())
                        .orElseGet(identifier::getValue)))
                .orElseGet(() -> labelNames.map(names -> range.map(range -> names.toString() + range.toString())
                        .orElseGet(names::toString))
                        .orElseGet(() -> range.map(RelationshipRange::toString)
                                .orElse("")));
    }
}
