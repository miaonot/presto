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

public class Gremlins
        extends Statement
{
    private final Optional<List<String>> sentence;
    private final boolean withGremlinsOption;

    public Gremlins(Optional<List<String>> sentence, boolean withGremlinsOption)
    {
        this(Optional.empty(), sentence, withGremlinsOption);
    }

    public Gremlins(NodeLocation location, Optional<List<String>> sentence, boolean withGremlinsOption)
    {
        this(Optional.of(location), sentence, withGremlinsOption);
    }

    private Gremlins(Optional<NodeLocation> location, Optional<List<String>> sentence, boolean withGremlinsOption)
    {
        super(location);
        requireNonNull(sentence, "sentence is null");
        this.sentence = sentence.map(ImmutableList::copyOf);
        this.withGremlinsOption = withGremlinsOption;
    }

    public Optional<List<String>> getSentence()
    {
        return sentence;
    }

    public boolean isWithGremlinsOption()
    {
        return withGremlinsOption;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGremlins(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sentence, withGremlinsOption);
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
        Gremlins o = (Gremlins) obj;
        return Objects.equals(sentence, o.sentence) &&
                Objects.equals(withGremlinsOption, o.withGremlinsOption);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sentence", sentence)
                .toString();
    }
}
