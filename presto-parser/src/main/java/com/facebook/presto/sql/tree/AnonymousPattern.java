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

public class AnonymousPattern
        extends Node
{
    NodePattern nodePattern;
    Optional<List<PatternElements>> elements;

    public AnonymousPattern(NodePattern nodePattern, Optional<List<PatternElements>> elements)
    {
        this(Optional.empty(), nodePattern, elements);
    }

    public AnonymousPattern(NodeLocation location, NodePattern nodePattern, Optional<List<PatternElements>> elements)
    {
        this(Optional.of(location), nodePattern, elements);
    }

    public AnonymousPattern(Optional<NodeLocation> location, NodePattern nodePattern, Optional<List<PatternElements>> elements)
    {
        super(location);
        this.nodePattern = requireNonNull(nodePattern, "nodePattern is null");
        this.elements = elements;
    }

    public NodePattern getNodePattern()
    {
        return nodePattern;
    }

    public Optional<List<PatternElements>> getElements()
    {
        return elements;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAnonymousPattern(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(nodePattern);
        elements.ifPresent(nodes::addAll);
        return nodes.build();
    }

    public List<String> getNodeTypes()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.add(nodePattern.getLabelNames().map(LabelNames::toString).orElse(""));
        if (elements.isPresent()) {
            for (PatternElements element : elements.get()) {
                builder.add(element.getNodePattern().getLabelNames().map(LabelNames::toString).orElse(""));
            }
        }
        return builder.build();
    }

    public List<String> getRelationshipTypes()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        if (elements.isPresent()) {
            for (PatternElements element : elements.get()) {
                builder.add(element.getRelationshipPattern().getDetail().getLabelNames().map(LabelNames::toString).orElse(""));
            }
        }
        return builder.build();
    }

    public List<String> getNodeNames()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.add(nodePattern.getNodeName().map(Identifier::toString).orElse(""));
        if (elements.isPresent()) {
            for (PatternElements elements : elements.get()) {
                builder.add(elements.getNodePattern().getNodeName().map(Identifier::toString).orElse(""));
            }
        }
        return builder.build();
    }

    public List<String> getRelationshipNames()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        if (elements.isPresent()) {
            for (PatternElements elements : elements.get()) {
                builder.add(elements.getRelationshipPattern().getDetail().getRelationName().map(Identifier::toString).orElse(""));
            }
        }
        return builder.build();
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
        AnonymousPattern o = (AnonymousPattern) obj;
        return nodePattern.equals(o.nodePattern) &&
                elements.equals(o.elements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodePattern, elements);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(nodePattern.toString());
        elements.ifPresent(patternElements -> patternElements.forEach(patternElement -> builder.append(patternElement.toString())));
        return builder.toString();
    }
}
