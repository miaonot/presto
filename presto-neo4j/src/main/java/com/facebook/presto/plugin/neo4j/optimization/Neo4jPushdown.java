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

import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.expressions.translator.TranslatedExpression;
import com.facebook.presto.plugin.neo4j.Neo4jTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MatchNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;

import static com.facebook.presto.expressions.translator.FunctionTranslator.buildFunctionTranslator;
import static com.facebook.presto.expressions.translator.RowExpressionTreeTranslator.translateWith;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static java.util.Objects.requireNonNull;

public class Neo4jPushdown
        implements ConnectorPlanOptimizer
{
    private final ExpressionOptimizer expressionOptimizer;
    private final LogicalRowExpressions logicalRowExpressions;
    private final JdbcFilterToCypherTranslator jdbcFilterToCypherTranslator;

    public Neo4jPushdown(
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution functionResolution,
            DeterminismEvaluator determinismEvaluator,
            ExpressionOptimizer expressionOptimizer,
            Set<Class<?>> functionTranslators)
    {
        requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        requireNonNull(functionTranslators, "functionTranslators is null");
        requireNonNull(determinismEvaluator, "determinismEvaluator is null");
        requireNonNull(functionResolution, "functionResolution is null");

        this.expressionOptimizer = requireNonNull(expressionOptimizer, "expressionOptimizer is null");
        this.jdbcFilterToCypherTranslator = new JdbcFilterToCypherTranslator(
                functionMetadataManager,
                buildFunctionTranslator(functionTranslators));
        this.logicalRowExpressions = new LogicalRowExpressions(
                determinismEvaluator,
                functionResolution,
                functionMetadataManager);
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return maxSubplan.accept(new Visitor(session, idAllocator), null);
    }

    private class Visitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final ConnectorSession session;
        private final PlanNodeIdAllocator idAllocator;

        public Visitor(ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            ImmutableList.Builder<PlanNode> children = ImmutableList.builder();
            boolean changed = false;
            for (PlanNode child : node.getSources()) {
                PlanNode newChild = child.accept(this, null);
                if (newChild != child) {
                    changed = true;
                }
                children.add(newChild);
            }

            if (!changed) {
                return node;
            }
            return node.replaceChildren(children.build());
        }

        @Override
        public PlanNode visitProject(ProjectNode node, Void context)
        {
            if (!(node.getSource() instanceof MatchNode)) {
                ImmutableList.Builder<PlanNode> children = ImmutableList.builder();
                boolean changed = false;
                for (PlanNode child : node.getSources()) {
                    PlanNode newChild = child.accept(this, null);
                    if (newChild != child) {
                        changed = true;
                    }
                    children.add(newChild);
                }

                if (!changed) {
                    return node;
                }

                node = (ProjectNode) node.replaceChildren(children.build());
                if (!(node.getSource() instanceof MatchNode)) {
                    return node;
                }
            }

            if (node.getOutputVariables().size() == 0) {
                return node;
            }

            MatchNode oldMatchNode = (MatchNode) node.getSource();
            TableHandle oldTableHandle = oldMatchNode.getTable();
            Neo4jTableHandle oldConnectorHandle = (Neo4jTableHandle) oldTableHandle.getConnectorHandle();

            Map<VariableReferenceExpression, ColumnHandle> oldAssignments = oldMatchNode.getAssignments();
            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> newAssignmentsBuilder = ImmutableMap.builder();

            ImmutableList.Builder<String> builder = ImmutableList.builder();

            for (VariableReferenceExpression expression : node.getOutputVariables()) {
                String name = expression.getName();
                builder.add(name);
                newAssignmentsBuilder.put(expression, oldAssignments.get(expression));
            }

            Neo4jTableHandle newConnectorHandle = Neo4jTableHandle.setProject(oldConnectorHandle, builder.build());
            TableHandle newTableHandle = new TableHandle(oldTableHandle.getConnectorId(), newConnectorHandle, oldTableHandle.getTransaction(), oldTableHandle.getLayout());

            return new MatchNode(idAllocator.getNextId(), newTableHandle, oldMatchNode.getMatchStatement(), node.getOutputVariables(), newAssignmentsBuilder.build());
        }

        @Override
        public PlanNode visitLimit(LimitNode node, Void context)
        {
            if (!(node.getSource() instanceof MatchNode)) {
                ImmutableList.Builder<PlanNode> children = ImmutableList.builder();
                boolean changed = false;
                for (PlanNode child : node.getSources()) {
                    PlanNode newChild = child.accept(this, null);
                    if (newChild != child) {
                        changed = true;
                    }
                    children.add(newChild);
                }

                if (!changed) {
                    return node;
                }

                node = (LimitNode) node.replaceChildren(children.build());
                if (!(node.getSource() instanceof MatchNode)) {
                    return node;
                }
            }

            MatchNode oldMatchNode = (MatchNode) node.getSource();
            TableHandle oldTableHandle = oldMatchNode.getTable();
            Neo4jTableHandle oldConnectorHandle = (Neo4jTableHandle) oldTableHandle.getConnectorHandle();

            Neo4jTableHandle newConnectorHandle = Neo4jTableHandle.setLimitCount(oldConnectorHandle, node.getCount());
            TableHandle newTableHandle = new TableHandle(oldTableHandle.getConnectorId(), newConnectorHandle, oldTableHandle.getTransaction(), oldTableHandle.getLayout());

            return new MatchNode(idAllocator.getNextId(), newTableHandle, oldMatchNode.getMatchStatement(), oldMatchNode.getOutputVariables(), oldMatchNode.getAssignments());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            if (!(node.getSource() instanceof MatchNode)) {
                ImmutableList.Builder<PlanNode> children = ImmutableList.builder();
                boolean changed = false;
                for (PlanNode child : node.getSources()) {
                    PlanNode newChild = child.accept(this, null);
                    if (newChild != child) {
                        changed = true;
                    }
                    children.add(newChild);
                }

                if (!changed) {
                    return node;
                }

                node = (FilterNode) node.replaceChildren(children.build());
                if (!(node.getSource() instanceof MatchNode)) {
                    return node;
                }
            }

            MatchNode oldMatchNode = (MatchNode) node.getSource();

            RowExpression predicate = expressionOptimizer.optimize(node.getPredicate(), OPTIMIZED, session);
            predicate = logicalRowExpressions.convertToConjunctiveNormalForm(predicate);
            TranslatedExpression<CypherExpression> cypherExpression = translateWith(
                    predicate,
                    jdbcFilterToCypherTranslator,
                    oldMatchNode.getAssignments());

            TableHandle oldTableHandle = oldMatchNode.getTable();
            if (!cypherExpression.getTranslated().isPresent()) {
                return node;
            }

            Neo4jTableHandle oldConnectorHandle = (Neo4jTableHandle) oldTableHandle.getConnectorHandle();

            Neo4jTableHandle newConnectorHandle = Neo4jTableHandle.setPredicate(oldConnectorHandle, cypherExpression.getTranslated().get());
            TableHandle newTableHandle = new TableHandle(oldTableHandle.getConnectorId(), newConnectorHandle, oldTableHandle.getTransaction(), oldTableHandle.getLayout());

            return new MatchNode(idAllocator.getNextId(), newTableHandle, oldMatchNode.getMatchStatement(), oldMatchNode.getOutputVariables(), oldMatchNode.getAssignments());
        }
    }
}
