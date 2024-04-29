/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.kubeclient.services;

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.Endpoint;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesIngress;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRule;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.apache.flink.kubernetes.utils.Constants.FLINK_REST_SERVICE_SUFFIX;

/** The service type of ClusterIP. */
public class ClusterIPService extends ServiceType {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterIPService.class);

    public static final ClusterIPService INSTANCE = new ClusterIPService();

    @Override
    public Service buildUpInternalService(
            KubernetesJobManagerParameters kubernetesJobManagerParameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Endpoint> getRestEndpoint(
            Service targetService,
            NamespacedKubernetesClient internalClient,
            KubernetesConfigOptions.NodePortAddressType nodePortAddressType) {
        String serviceName = targetService.getMetadata().getName();
        String clusterId =
                serviceName.endsWith(FLINK_REST_SERVICE_SUFFIX)
                        ? serviceName.substring(
                                0, serviceName.length() - FLINK_REST_SERVICE_SUFFIX.length())
                        : serviceName;
        Optional<KubernetesIngress> serviceIngress =
                getServiceIngress(targetService, internalClient, clusterId);
        LOG.warn(
                String.format(
                        "getRestEndpoint_Ingress_Try: [%s] -> %s",
                        targetService.getMetadata().getNamespace(), clusterId));
        if (serviceIngress.isPresent()) {
            LOG.warn(
                    String.format(
                            "getRestEndpoint_Ingress_Success: [%s] -> %s",
                            targetService.getMetadata().getNamespace(), clusterId));
            try {
                KubernetesIngress ingress = serviceIngress.get();
                IngressRule rule = ingress.getInternalResource().getSpec().getRules().get(0);
                String host =
                        String.format(
                                "http://%s%s",
                                rule.getHost(), rule.getHttp().getPaths().get(0).getPath());
                return Optional.of(new Endpoint(host, -1));
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        String.format("getRestEndpoint_Ingress_Error: %s", clusterId), e);
            }
        } else {
            LOG.warn(
                    String.format(
                            "getRestEndpoint_Ingress_Try_Not_Found: [%s] -> %s",
                            targetService.getMetadata().getNamespace(), clusterId));
            int restPort = getRestPortFromExternalService(targetService);

            // Return the external service.namespace directly when using ClusterIP.
            return Optional.of(
                    new Endpoint(
                            KubernetesUtils.getNamespacedServiceName(targetService), restPort));
        }
    }

    private Optional<KubernetesIngress> getServiceIngress(
            Service targetService, NamespacedKubernetesClient internalClient, String clusterId) {
        Ingress ingress =
                internalClient
                        .network()
                        .v1()
                        .ingresses()
                        .inNamespace(targetService.getMetadata().getNamespace())
                        .withName(clusterId)
                        .get();
        if (ingress == null) {
            LOG.warn("Ingress {} does not exist", clusterId);
            return Optional.empty();
        }
        return Optional.of(new KubernetesIngress(ingress));
    }

    @Override
    public int getRestPort(ServicePort port) {
        return port.getPort();
    }

    @Override
    public String getType() {
        return KubernetesConfigOptions.ServiceExposedType.ClusterIP.name();
    }
}
