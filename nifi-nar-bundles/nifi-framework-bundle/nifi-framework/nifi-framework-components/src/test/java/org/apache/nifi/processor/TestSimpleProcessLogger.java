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
package org.apache.nifi.processor;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepository;
import org.apache.nifi.logging.PerProcessGroupLoggable;
import org.apache.nifi.logging.StandardLoggingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.event.Level;
import org.slf4j.spi.DefaultLoggingEventBuilder;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestSimpleProcessLogger {

    private static final String FIRST_MESSAGE = "FIRST";
    private static final String SECOND_MESSAGE = "SECOND";
    private static final String THIRD_MESSAGE = "THIRD";

    private static final String EXPECTED_CAUSES = String.join(System.lineSeparator(),
            String.format("%s: %s", IllegalArgumentException.class.getName(), FIRST_MESSAGE),
            String.format("- Caused by: %s: %s", RuntimeException.class.getName(), SECOND_MESSAGE),
            String.format("- Caused by: %s: %s", SecurityException.class.getName(), THIRD_MESSAGE)
    );

    private static final Exception EXCEPTION = new IllegalArgumentException(FIRST_MESSAGE, new RuntimeException(SECOND_MESSAGE, new SecurityException(THIRD_MESSAGE)));

    private static final String EXCEPTION_STRING = EXCEPTION.toString();

    private static final Throwable NULL_THROWABLE = null;

    private static final String FIRST = "FIRST";

    private static final int SECOND = 2;

    private static final Object[] VALUE_ARGUMENTS = new Object[]{FIRST, SECOND};

    private static final Object[] VALUE_EXCEPTION_ARGUMENTS = new Object[]{FIRST, SECOND, EXCEPTION};

    private static final String LOG_MESSAGE = "Processed";

    private static final String LOG_MESSAGE_WITH_COMPONENT = String.format("{} %s", LOG_MESSAGE);

    private static final String LOG_MESSAGE_WITH_COMPONENT_AND_CAUSES = String.format("{} %s: {}", LOG_MESSAGE);

    private static final String LOG_ARGUMENTS_MESSAGE = "Processed {} {}";

    private static final String LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT = String.format("{} %s", LOG_ARGUMENTS_MESSAGE);

    private static final String LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT_AND_CAUSES = String.format("{} %s: {}", LOG_ARGUMENTS_MESSAGE);

    private static final String DISCRIMINATOR_KEY = "logFileSuffix";

    private static final String LOG_FILE_SUFFIX = "myGroup";

    @Mock
    private ConfigurableComponent component;

    @Mock
    private LogRepository logRepository;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Logger logger;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    DefaultLoggingEventBuilder loggingEventBuilder;

    @Mock
    StandardLoggingContext<PerProcessGroupLoggable> loggingContext;

    private Object[] componentArguments;

    private Object[] componentValueArguments;

    private Object[] componentValueCausesArguments;

    private Object[] componentCausesArguments;

    private SimpleProcessLogger componentLog;

    @BeforeEach
    public void setLogger() throws IllegalAccessException {
        componentLog = new SimpleProcessLogger(component, logRepository, loggingContext);
        FieldUtils.writeDeclaredField(componentLog, "logger", logger, true);

        componentArguments = new Object[]{component};
        componentValueArguments = new Object[]{component, FIRST, SECOND};
        componentValueCausesArguments = new Object[]{component, FIRST, SECOND, EXPECTED_CAUSES};
        componentCausesArguments = new Object[]{component, EXPECTED_CAUSES};

        when(logger.isTraceEnabled()).thenReturn(true);
        when(logger.isDebugEnabled()).thenReturn(true);
        when(logger.isInfoEnabled()).thenReturn(true);
        when(logger.isWarnEnabled()).thenReturn(true);
        when(logger.isErrorEnabled()).thenReturn(true);
        when(logger.makeLoggingEventBuilder(any(Level.class))).thenReturn(loggingEventBuilder);
        when(loggingContext.getDiscriminatorKey()).thenReturn(DISCRIMINATOR_KEY);
        when(loggingContext.getLogFileSuffix()).thenReturn(Optional.of(LOG_FILE_SUFFIX));
        when(loggingEventBuilder.setMessage(anyString())
                .addArgument(any(Object.class))
                .addKeyValue(any(String.class), any(String.class)))
                .thenReturn(loggingEventBuilder);
    }

    @Test
    public void testLogLevelMessage() {
        for (final LogLevel logLevel : LogLevel.values()) {
            componentLog.log(logLevel, LOG_MESSAGE);

            switch (logLevel) {
                case TRACE:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.TRACE));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(component));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case DEBUG:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.DEBUG));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(component));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case INFO:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.INFO));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(component));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case WARN:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.WARN));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(component));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case ERROR:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.ERROR));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(component));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                default:
                    continue;
            }

            verify(logRepository).addLogMessage(eq(logLevel), eq(LOG_MESSAGE_WITH_COMPONENT), eq(componentArguments));
        }
    }

    @Test
    public void testLogLevelMessageArguments() {
        final Object[] LOGGABLE_ARGUMENTS = new Object[] { component, FIRST, SECOND };
        for (final LogLevel logLevel : LogLevel.values()) {
            componentLog.log(logLevel, LOG_ARGUMENTS_MESSAGE, VALUE_ARGUMENTS);

            switch (logLevel) {
                case TRACE:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.TRACE));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(LOGGABLE_ARGUMENTS));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case DEBUG:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.DEBUG));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(LOGGABLE_ARGUMENTS));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case INFO:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.INFO));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(LOGGABLE_ARGUMENTS));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case WARN:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.WARN));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(LOGGABLE_ARGUMENTS));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case ERROR:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.ERROR));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(LOGGABLE_ARGUMENTS));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                default:
                    continue;
            }

            verify(logRepository).addLogMessage(eq(logLevel), eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), eq(componentValueArguments));
        }
    }

    @Test
    public void testLogLevelMessageThrowable() {
        for (final LogLevel logLevel : LogLevel.values()) {
            componentLog.log(logLevel, LOG_MESSAGE, EXCEPTION);

            switch (logLevel) {
                case TRACE:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.TRACE));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(component));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .setCause(eq(EXCEPTION));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX)
                            .setCause(EXCEPTION), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case DEBUG:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.DEBUG));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(component));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .setCause(eq(EXCEPTION));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX)
                            .setCause(EXCEPTION), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case INFO:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.INFO));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(component));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .setCause(eq(EXCEPTION));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX)
                            .setCause(EXCEPTION), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case WARN:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.WARN));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(component));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .setCause(eq(EXCEPTION));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX)
                            .setCause(EXCEPTION), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case ERROR:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.ERROR));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(component));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .setCause(eq(EXCEPTION));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_MESSAGE_WITH_COMPONENT)
                            .addArgument(component)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX)
                            .setCause(EXCEPTION), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                default:
                    continue;
            }

            verify(logRepository).addLogMessage(eq(logLevel), eq(LOG_MESSAGE_WITH_COMPONENT_AND_CAUSES), eq(componentCausesArguments), eq(EXCEPTION));
        }
    }

    @Test
    public void testLogLevelMessageArgumentsThrowable() {
        final Object[] LOGGABLE_ARGUMENTS = new Object[] { component, FIRST, SECOND, EXCEPTION_STRING, EXCEPTION};
        for (final LogLevel logLevel : LogLevel.values()) {
            componentLog.log(logLevel, LOG_ARGUMENTS_MESSAGE, VALUE_EXCEPTION_ARGUMENTS);

            switch (logLevel) {
                case TRACE:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.TRACE));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(LOGGABLE_ARGUMENTS));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .setCause(eq(EXCEPTION));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX)
                            .setCause(EXCEPTION), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case DEBUG:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.DEBUG));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(LOGGABLE_ARGUMENTS));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .setCause(eq(EXCEPTION));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX)
                            .setCause(EXCEPTION), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case INFO:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.INFO));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(LOGGABLE_ARGUMENTS));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .setCause(eq(EXCEPTION));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX)
                            .setCause(EXCEPTION), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case WARN:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.WARN));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(LOGGABLE_ARGUMENTS));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .setCause(eq(EXCEPTION));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX)
                            .setCause(EXCEPTION), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case ERROR:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.ERROR));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(LOGGABLE_ARGUMENTS));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .setCause(eq(EXCEPTION));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX)
                            .setCause(EXCEPTION), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                default:
                    continue;
            }

            verify(logRepository).addLogMessage(eq(logLevel), eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT_AND_CAUSES), eq(componentValueCausesArguments), eq(EXCEPTION));
        }
    }

    @Test
    public void testLogLevelMessageArgumentsArrayThrowable() {
        for (final LogLevel logLevel : LogLevel.values()) {
            componentLog.log(logLevel, LOG_ARGUMENTS_MESSAGE, VALUE_ARGUMENTS, EXCEPTION);

            switch (logLevel) {
                case TRACE:
                    verify(logger).trace(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), eq(component), eq(FIRST), eq(SECOND), eq(EXCEPTION));
                    break;
                case DEBUG:
                    verify(logger).debug(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), eq(component), eq(FIRST), eq(SECOND), eq(EXCEPTION));
                    break;
                case INFO:
                    verify(logger).info(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), eq(component), eq(FIRST), eq(SECOND), eq(EXCEPTION));
                    break;
                case WARN:
                    verify(logger).warn(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), eq(component), eq(FIRST), eq(SECOND), eq(EXCEPTION));
                    break;
                case ERROR:
                    verify(logger).error(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), eq(component), eq(FIRST), eq(SECOND), eq(EXCEPTION));
                    break;
                default:
                    continue;
            }

            verify(logRepository).addLogMessage(eq(logLevel), eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT_AND_CAUSES), eq(componentValueCausesArguments), eq(EXCEPTION));
        }
    }

    @Test
    public void testLogLevelMessageArgumentsThrowableNull() {
        final Object[] LOGGABLE_ARGUMENTS = new Object[] { component, FIRST, SECOND };
        for (final LogLevel logLevel : LogLevel.values()) {
            componentLog.log(logLevel, LOG_ARGUMENTS_MESSAGE, VALUE_ARGUMENTS, NULL_THROWABLE);

            switch (logLevel) {
                case TRACE:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.TRACE));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(LOGGABLE_ARGUMENTS));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case DEBUG:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.DEBUG));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(LOGGABLE_ARGUMENTS));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case INFO:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.INFO));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(LOGGABLE_ARGUMENTS));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case WARN:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.WARN));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(LOGGABLE_ARGUMENTS));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                case ERROR:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.ERROR));
                    verify(loggingEventBuilder, times(1))
                            .setMessage(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), times(1))
                            .addArgument(eq(LOGGABLE_ARGUMENTS));
                    verify(loggingEventBuilder
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS), times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(logger.makeLoggingEventBuilder(Level.TRACE)
                            .setMessage(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT)
                            .addArgument(LOGGABLE_ARGUMENTS)
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log();
                    reset(loggingEventBuilder);
                    break;
                default:
                    continue;
            }

            verify(logRepository).addLogMessage(eq(logLevel), eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), eq(componentValueArguments));
        }
    }
}
