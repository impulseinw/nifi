/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.attribute.expression.language;

import org.antlr.runtime.tree.Tree;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.literals.StringLiteralEvaluator;
import org.apache.nifi.expression.AttributeValueDecorator;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class StringLiteralExpression implements CompiledExpression {
    private final String value;
    private final Evaluator<String> rootEvaluator;

    public StringLiteralExpression(final String value) {
        this.value = value;
        rootEvaluator = new StringLiteralEvaluator(value);
    }

    @Override
    public String evaluate(Map<String, String> variables, AttributeValueDecorator decorator, Map<String, String> stateVariables) {
        return value;
    }

    @Override
    public Evaluator<?> getRootEvaluator() {
        return rootEvaluator;
    }

    @Override
    public Tree getTree() {
        // TODO: Make sure this is okay.
        return null;
    }

    @Override
    public String getExpression() {
        return value;
    }

    @Override
    public Set<Evaluator<?>> getAllEvaluators() {
        return Collections.singleton(rootEvaluator);
    }
}
