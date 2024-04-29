package org.apache.flink.kubernetes.kubeclient.resources;

import io.fabric8.kubernetes.api.model.networking.v1.Ingress;

/** Represent Ingress resource in kubernetes. */
public class KubernetesIngress extends KubernetesResource<Ingress> {

    public KubernetesIngress(Ingress internalResource) {
        super(internalResource);
    }
}
