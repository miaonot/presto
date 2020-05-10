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

public class PatternElements
        extends Node
{
    RelationshipPattern relationshipPattern;
    NodePattern nodePattern;

    public PatternElements(RelationshipPattern relationshipPattern, NodePattern nodePattern)
    {
        this(Optional.empty(), relationshipPattern, nodePattern);
    }

    public PatternElements(NodeLocation location, RelationshipPattern relationshipPattern, NodePattern nodePattern)
    {
        this(Optional.of(location), relationshipPattern, nodePattern);
    }

    public PatternElements(Optional<NodeLocation> location, RelationshipPattern relationshipPattern, NodePattern nodePattern)
    {
        super(location);
        this.relationshipPattern = relationshipPattern;
        this.nodePattern = nodePattern;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPatternElements(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(relationshipPattern, nodePattern);
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
        PatternElements o = (PatternElements) obj;
        return relationshipPattern.equals(o.relationshipPattern) &&
                nodePattern.equals(o.nodePattern);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relationshipPattern, nodePattern);
    }

    @Override
    public String toString()
    {
        return relationshipPattern.toString() + nodePattern.toString();
    }
}
