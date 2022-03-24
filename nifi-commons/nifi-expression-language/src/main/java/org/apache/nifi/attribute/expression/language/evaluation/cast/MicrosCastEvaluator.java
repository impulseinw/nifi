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
package org.apache.nifi.attribute.expression.language.evaluation.cast;

import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.InstantQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.NumberEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.NumberQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.expression.AttributeExpression;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class MicrosCastEvaluator extends NumberEvaluator {

    private final Evaluator<?> subjectEvaluator;

    public MicrosCastEvaluator(final Evaluator<?> subjectEvaluator) {
        this.subjectEvaluator = subjectEvaluator;
    }

    @Override
    public QueryResult<Number> evaluate(EvaluationContext evaluationContext) {
        final QueryResult<?> result = subjectEvaluator.evaluate(evaluationContext);
        if (result.getValue() == null) {
            return new NumberQueryResult(null);
        }

        if (result.getResultType() == AttributeExpression.ResultType.INSTANT) {
            final Instant instant = ((InstantQueryResult) result).getValue();
            long micros = ChronoUnit.MICROS.between(Instant.EPOCH, instant);
            return new NumberQueryResult(micros);
        }
        return new NumberQueryResult(null);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subjectEvaluator;
    }
}
