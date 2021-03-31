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

public class GraphFunc
        extends Node
{
    List<Expression> arguments;

    public GraphFunc(List<Expression> arguments)
    {
        this(Optional.empty(), arguments);
    }

    public GraphFunc(NodeLocation location, List<Expression> arguments)
    {
        this(Optional.of(location), arguments);
    }

    public GraphFunc(Optional<NodeLocation> location, List<Expression> arguments)
    {
        super(location);
        this.arguments = requireNonNull(arguments, "token is null");
    }

    public List<Expression> getExpressions()
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        builder.addAll(arguments);
        return builder.build();
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(arguments);
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
        GraphFunc o = (GraphFunc) obj;
        return arguments.equals(o.arguments);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("args", arguments)
                .toString();
    }
}
