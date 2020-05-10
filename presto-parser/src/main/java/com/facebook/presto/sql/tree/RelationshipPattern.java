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

import static java.util.Objects.requireNonNull;

public class RelationshipPattern
        extends Node
{
    String lt;
    String rt;
    RelationshipDetail detail;

    public RelationshipPattern(String lt, String rt, RelationshipDetail detail)
    {
        this(Optional.empty(), lt, rt, detail);
    }

    public RelationshipPattern(NodeLocation location, String lt, String rt, RelationshipDetail detail)
    {
        this(Optional.of(location), lt, rt, detail);
    }

    public RelationshipPattern(Optional<NodeLocation> location, String lt, String rt, RelationshipDetail detail)
    {
        super(location);
        this.lt = requireNonNull(lt, "lt is null");
        this.rt = requireNonNull(rt, "rt is null");
        this.detail = requireNonNull(detail, "relationshipDetail is null");
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRelationshipPattern(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(detail);
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
        RelationshipPattern o = (RelationshipPattern) obj;
        return lt.equals(o.lt) &&
                rt.equals(o.rt) &&
                detail.equals(o.detail);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lt, rt, detail);
    }

    @Override
    public String toString()
    {
        return lt + "[" + detail.toString() + "]" + rt;
    }
}
