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
package com.facebook.presto.plugin.neo4j.optimization;

import com.facebook.presto.expressions.translator.FunctionTranslator;
import com.facebook.presto.expressions.translator.RowExpressionTranslator;
import com.facebook.presto.expressions.translator.RowExpressionTreeTranslator;
import com.facebook.presto.expressions.translator.TranslatedExpression;
import com.facebook.presto.plugin.neo4j.Neo4jColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.expressions.translator.TranslatedExpression.untranslated;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JdbcFilterToCypherTranslator
        extends RowExpressionTranslator<CypherExpression, Map<VariableReferenceExpression, ColumnHandle>>
{
    private final FunctionMetadataManager functionMetadataManager;
    private final FunctionTranslator<CypherExpression> functionTranslator;

    public JdbcFilterToCypherTranslator(FunctionMetadataManager functionMetadataManager, FunctionTranslator<CypherExpression> functionTranslator)
    {
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.functionTranslator = requireNonNull(functionTranslator, "functionTranslator is null");
    }

    @Override
    public TranslatedExpression<CypherExpression> translateConstant(ConstantExpression literal, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<CypherExpression, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
    {
        if (literal.getType() instanceof VarcharType || literal.getType() instanceof CharType) {
            return new TranslatedExpression<>(
                    Optional.of(new CypherExpression(((Slice) literal.getValue()).toStringUtf8(), ImmutableList.of(literal))),
                    literal,
                    ImmutableList.of());
        }
        else {
            return new TranslatedExpression<>(
                    Optional.of(new CypherExpression(literal.getValue().toString(), ImmutableList.of(literal))),
                    literal,
                    ImmutableList.of());
        }
    }

    @Override
    public TranslatedExpression<CypherExpression> translateVariable(VariableReferenceExpression variable, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<CypherExpression, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
    {
        Neo4jColumnHandle columnHandle = (Neo4jColumnHandle) context.get(variable);
        requireNonNull(columnHandle, format("Unrecognized variable %s", variable));
        return new TranslatedExpression<>(
                Optional.of(new CypherExpression(new StringBuilder(columnHandle.getColumnName()).insert(columnHandle.getColumnName().startsWith("node") ? 5 : 13, ".").toString())),
                variable,
                ImmutableList.of());
    }

    @Override
    public TranslatedExpression<CypherExpression> translateLambda(LambdaDefinitionExpression lambda, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<CypherExpression, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
    {
        return super.translateLambda(lambda, context, rowExpressionTreeTranslator);
    }

    @Override
    public TranslatedExpression<CypherExpression> translateCall(CallExpression call, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<CypherExpression, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
    {
        List<TranslatedExpression<CypherExpression>> translatedExpressions = call.getArguments().stream()
                .map(expression -> rowExpressionTreeTranslator.rewrite(expression, context))
                .collect(toImmutableList());

        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(call.getFunctionHandle());

        try {
            return functionTranslator.translate(functionMetadata, call, translatedExpressions);
        }
        catch (Throwable t) {
            // no-op
        }
        return untranslated(call, translatedExpressions);
    }

    @Override
    public TranslatedExpression<CypherExpression> translateSpecialForm(SpecialFormExpression specialForm, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<CypherExpression, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
    {
        List<TranslatedExpression<CypherExpression>> translatedExpressions = specialForm.getArguments().stream()
                .map(expression -> rowExpressionTreeTranslator.rewrite(expression, context))
                .collect(toImmutableList());

        List<CypherExpression> cypherExpressions = translatedExpressions.stream()
                .map(TranslatedExpression::getTranslated)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());

        if (cypherExpressions.size() < translatedExpressions.size()) {
            return untranslated(specialForm, translatedExpressions);
        }

        List<String> sqlBodies = cypherExpressions.stream()
                .map(CypherExpression::getExpression)
                .map(sql -> '(' + sql + ')')
                .collect(toImmutableList());
        List<ConstantExpression> variableBindings = cypherExpressions.stream()
                .map(CypherExpression::getBoundConstantValues)
                .flatMap(List::stream)
                .collect(toImmutableList());

        switch (specialForm.getForm()) {
            case AND:
                return new TranslatedExpression<>(
                        Optional.of(new CypherExpression(format("(%s)", Joiner.on(" AND ").join(sqlBodies)), variableBindings)),
                        specialForm,
                        translatedExpressions);
            case OR:
                return new TranslatedExpression<>(
                        Optional.of(new CypherExpression(format("(%s)", Joiner.on(" OR ").join(sqlBodies)), variableBindings)),
                        specialForm,
                        translatedExpressions);
            case IN:
                return new TranslatedExpression<>(
                        Optional.of(new CypherExpression(format("(%s IN [%s])", sqlBodies.get(0), Joiner.on(" , ").join(sqlBodies.subList(1, sqlBodies.size()))), variableBindings)),
                        specialForm,
                        translatedExpressions);
        }
        return untranslated(specialForm, translatedExpressions);
    }
}
