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

package org.apache.nifi.reporting.prometheus;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import io.prometheus.client.CollectorRegistry;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.metrics.jvm.JmxJvmMetrics;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.prometheus.api.PrometheusMetricsUtil;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.ssl.SSLContextService;
import org.eclipse.jetty.server.Server;

import static org.apache.nifi.reporting.prometheus.api.PrometheusMetricsUtil.METRICS_STRATEGY_COMPONENTS;
import static org.apache.nifi.reporting.prometheus.api.PrometheusMetricsUtil.METRICS_STRATEGY_PG;
import static org.apache.nifi.reporting.prometheus.api.PrometheusMetricsUtil.METRICS_STRATEGY_ROOT;

@Tags({ "reporting", "prometheus", "metrics", "time series data" })
@CapabilityDescription("Reports metrics in Prometheus format by creating /metrics http endpoint which can be used for external monitoring of the application."
        + " The reporting task reports a set of metrics regarding the JVM (optional) and the NiFi instance")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "60 sec")
public class PrometheusReportingTask extends AbstractReportingTask {

    private PrometheusServer prometheusServer;

    public static final PropertyDescriptor METRICS_STRATEGY = new PropertyDescriptor.Builder()
            .name("prometheus-reporting-task-metrics-strategy")
            .displayName("Metrics Reporting Strategy")
            .description("The granularity on which to report metrics. Options include only the root process group, all process groups, or all components")
            .allowableValues(METRICS_STRATEGY_ROOT, METRICS_STRATEGY_PG, METRICS_STRATEGY_COMPONENTS)
            .defaultValue(METRICS_STRATEGY_COMPONENTS.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor SEND_JVM_METRICS = new PropertyDescriptor.Builder()
            .name("prometheus-reporting-task-metrics-send-jvm")
            .displayName("Send JVM metrics")
            .description("Send JVM metrics in addition to the NiFi metrics")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PrometheusMetricsUtil.METRICS_ENDPOINT_PORT);
        props.add(PrometheusMetricsUtil.INSTANCE_ID);
        props.add(METRICS_STRATEGY);
        props.add(SEND_JVM_METRICS);
        props.add(PrometheusMetricsUtil.SSL_CONTEXT);
        props.add(PrometheusMetricsUtil.CLIENT_AUTH);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ConfigurationContext context) {
        SSLContextService sslContextService = context.getProperty(PrometheusMetricsUtil.SSL_CONTEXT).asControllerService(SSLContextService.class);
        final String metricsEndpointPort = context.getProperty(PrometheusMetricsUtil.METRICS_ENDPOINT_PORT).getValue();

        try {
            List<Function<ReportingContext, CollectorRegistry>> metricsCollectors = new ArrayList<>();
            if (sslContextService == null) {
                this.prometheusServer = new PrometheusServer(new InetSocketAddress(Integer.parseInt(metricsEndpointPort)), getLogger());
            } else {
                final String clientAuthValue = context.getProperty(PrometheusMetricsUtil.CLIENT_AUTH).getValue();
                final boolean need;
                final boolean want;
                if (PrometheusMetricsUtil.CLIENT_NEED.getValue().equals(clientAuthValue)) {
                    need = true;
                    want = false;
                } else if (PrometheusMetricsUtil.CLIENT_WANT.getValue().equals(clientAuthValue)) {
                    need = false;
                    want = true;
                } else {
                    need = false;
                    want = false;
                }
                this.prometheusServer = new PrometheusServer(Integer.parseInt(metricsEndpointPort), sslContextService, getLogger(), need, want);
            }
            Function<ReportingContext, CollectorRegistry> nifiMetrics = (reportingContext) -> {
                ProcessGroupStatus rootGroupStatus = reportingContext.getEventAccess().getControllerStatus();
                String instanceId = reportingContext.getProperty(PrometheusMetricsUtil.INSTANCE_ID).evaluateAttributeExpressions().getValue();
                String metricsStrategy = reportingContext.getProperty(METRICS_STRATEGY).getValue();
                return PrometheusMetricsUtil.createNifiMetrics(rootGroupStatus, instanceId, "", "RootProcessGroup", metricsStrategy);
            };
            metricsCollectors.add(nifiMetrics);
            if (context.getProperty(SEND_JVM_METRICS).asBoolean()) {
                Function<ReportingContext, CollectorRegistry> jvmMetrics = (reportingContext) -> {
                    String instanceId = reportingContext.getProperty(PrometheusMetricsUtil.INSTANCE_ID).evaluateAttributeExpressions().getValue();
                    return PrometheusMetricsUtil.createJvmMetrics(JmxJvmMetrics.getInstance(), instanceId);
                };
                metricsCollectors.add(jvmMetrics);
            }
            this.prometheusServer.setMetricsCollectors(metricsCollectors);
            getLogger().info("Started JETTY server");
        } catch (Exception e) {
            getLogger().error("Failed to start Jetty server", e);
        }
    }

    @OnStopped
    public void OnStopped() throws Exception {
        Server server = this.prometheusServer.getServer();
        server.stop();
    }

    @OnShutdown
    public void onShutDown() throws Exception {
        Server server = prometheusServer.getServer();
        server.stop();
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        this.prometheusServer.setReportingContext(context);
    }
}
