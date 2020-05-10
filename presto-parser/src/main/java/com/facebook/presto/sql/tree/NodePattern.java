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

public class NodePattern
        extends Node
{
    private final Optional<Identifier> nodeName;
    private final Optional<LabelNames> labelNames;

    public NodePattern(Optional<Identifier> nodeName, Optional<LabelNames> labelNames)
    {
        this(Optional.empty(), nodeName, labelNames);
    }

    public NodePattern(NodeLocation location, Optional<Identifier> nodeName, Optional<LabelNames> labelNames)
    {
        this(Optional.of(location), nodeName, labelNames);
    }

    public NodePattern(Optional<NodeLocation> location, Optional<Identifier> nodeName, Optional<LabelNames> labelNames)
    {
        super(location);
        this.nodeName = requireNonNull(nodeName, "nodeName is null");
        this.labelNames = labelNames;
    }

    public Optional<LabelNames> getLabelNames()
    {
        return labelNames;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitNodePattern(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodeName.ifPresent(nodes::add);
        labelNames.ifPresent(nodes::add);
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
        NodePattern o = (NodePattern) obj;
        return nodeName.equals(o.nodeName) &&
                labelNames.equals(o.labelNames);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeName, labelNames);
    }

    @Override
    public String toString()
    {
        return "(" +
                nodeName.map(identifier -> labelNames.map(names -> identifier.getValue() + names.toString())
                .orElseGet(identifier::getValue))
                .orElseGet(() -> labelNames.map(LabelNames::toString)
                        .orElse("")) +
                ")";
    }
}
