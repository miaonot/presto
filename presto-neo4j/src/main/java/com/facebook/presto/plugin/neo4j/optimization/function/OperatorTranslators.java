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
package com.facebook.presto.plugin.neo4j.optimization.function;

import com.facebook.presto.plugin.neo4j.optimization.CypherExpression;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import static com.facebook.presto.plugin.neo4j.optimization.function.CypherTranslationUtil.forwardBindVariables;
import static com.facebook.presto.plugin.neo4j.optimization.function.CypherTranslationUtil.infixOperation;
import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;

public class OperatorTranslators
{
    private OperatorTranslators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.BIGINT)
    public static CypherExpression add(@SqlType(StandardTypes.BIGINT) CypherExpression left, @SqlType(StandardTypes.BIGINT) CypherExpression right)
    {
        return new CypherExpression(infixOperation("+", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.BIGINT)
    public static CypherExpression subtract(@SqlType(StandardTypes.BIGINT) CypherExpression left, @SqlType(StandardTypes.BIGINT) CypherExpression right)
    {
        return new CypherExpression(infixOperation("-", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static CypherExpression bigintEqual(@SqlType(StandardTypes.BIGINT) CypherExpression left, @SqlType(StandardTypes.BIGINT) CypherExpression right)
    {
        return new CypherExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static CypherExpression integerEqual(@SqlType(StandardTypes.INTEGER) CypherExpression left, @SqlType(StandardTypes.INTEGER) CypherExpression right)
    {
        return new CypherExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static CypherExpression doubleEqual(@SqlType(StandardTypes.DOUBLE) CypherExpression left, @SqlType(StandardTypes.DOUBLE) CypherExpression right)
    {
        return new CypherExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static CypherExpression charEqual(@SqlType(StandardTypes.CHAR) CypherExpression left, @SqlType(StandardTypes.CHAR) CypherExpression right)
    {
        return new CypherExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static CypherExpression varcharEqual(@SqlType(StandardTypes.VARCHAR) CypherExpression left, @SqlType(StandardTypes.VARCHAR) CypherExpression right)
    {
        return new CypherExpression(infixOperation("=", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static CypherExpression notEqual(@SqlType(StandardTypes.BIGINT) CypherExpression left, @SqlType(StandardTypes.BIGINT) CypherExpression right)
    {
        return new CypherExpression(infixOperation("<>", left, right), forwardBindVariables(left, right));
    }

    @ScalarFunction("not")
    @SqlType(StandardTypes.BOOLEAN)
    public static CypherExpression not(@SqlType(StandardTypes.BOOLEAN) CypherExpression expression)
    {
        return new CypherExpression(String.format("(NOT(%s))", expression.getExpression()), expression.getBoundConstantValues());
    }
}
