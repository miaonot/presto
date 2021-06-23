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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LabelNames
        extends Node
{
    List<LabelName> labelNames;

    public LabelNames(List<LabelName> labelNames)
    {
        this(Optional.empty(), labelNames);
    }

    public LabelNames(NodeLocation location, List<LabelName> labelNames)
    {
        this(Optional.of(location), labelNames);
    }

    public LabelNames(Optional<NodeLocation> location, List<LabelName> labelNames)
    {
        super(location);
        this.labelNames = requireNonNull(labelNames, "labelNames is null");
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLabelNames(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return labelNames;
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
        LabelNames o = (LabelNames) obj;
        return labelNames.equals(o.labelNames);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(labelNames);
    }

    @Override
    public String toString()
    {
        StringBuilder string = new StringBuilder();
        for (LabelName labelName : labelNames) {
            string.append(labelName.toString());
        }
        return string.toString();
    }
}