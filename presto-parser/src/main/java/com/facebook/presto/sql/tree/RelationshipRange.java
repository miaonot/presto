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

public class RelationshipRange
        extends Node
{
    private final Optional<Integer> lower;
    private final Optional<Integer> higher;

    public RelationshipRange(Optional<Integer> lower, Optional<Integer> higher)
    {
        this(Optional.empty(), lower, higher);
    }

    public RelationshipRange(NodeLocation location, Optional<Integer> lower, Optional<Integer> higher)
    {
        this(Optional.of(location), lower, higher);
    }

    public RelationshipRange(Optional<NodeLocation> location, Optional<Integer> lower, Optional<Integer> higher)
    {
        super(location);
        this.lower = lower;
        this.higher = higher;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRelationshipRange(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
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
        RelationshipRange o = (RelationshipRange) obj;
        return lower.equals(o.lower) &&
                higher.equals(o.higher);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lower, higher);
    }

    @Override
    public String toString()
    {
        return lower.map(value -> higher.map(integer -> "*" + value + ".." + integer)
                .orElseGet(() -> "*" + value + ".."))
                .orElseGet(() -> "*" + higher.map(integer -> ".." + integer)
                        .orElse("*"));
    }
}
