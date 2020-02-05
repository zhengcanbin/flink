package org.apache.flink.kubernetes.kubeclient.builder;

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesMasterSpecification;
import org.apache.flink.kubernetes.kubeclient.conf.KubernetesMasterConf;
import org.apache.flink.kubernetes.kubeclient.conf.KubernetesTaskManagerConf;
import org.apache.flink.kubernetes.kubeclient.decorators.KubernetesStepDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.common.MountVolumesDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.jobmanager.FlinkConfConfigMapDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.jobmanager.InitJobManagerDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.jobmanager.RestServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.jobmanager.StartCommandMasterDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.taskmanager.InitTaskManagerDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.taskmanager.StartCommandDecorator;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class KubernetesComponentBuilder {

	public static KubernetesMasterSpecification buildJobManagerComponent(
			KubernetesMasterConf kubernetesMasterConf) throws IOException {
		FlinkPod flinkPod = new FlinkPodBuilder().build();
		List<HasMetadata> additionalResources = new ArrayList<>();

		final List<KubernetesStepDecorator> stepDecorators = Arrays.asList(
			new InitJobManagerDecorator(kubernetesMasterConf),
			new StartCommandMasterDecorator(kubernetesMasterConf),
			new RestServiceDecorator(kubernetesMasterConf),
			new MountVolumesDecorator(kubernetesMasterConf),
			new FlinkConfConfigMapDecorator(kubernetesMasterConf));

		for (KubernetesStepDecorator stepDecorator: stepDecorators) {
			flinkPod = stepDecorator.configureFlinkPod(flinkPod);
			additionalResources.addAll(stepDecorator.generateAdditionalKubernetesResources());
		}

		final Deployment deployment = buildJobManagerDeployment(flinkPod, kubernetesMasterConf);

		return new KubernetesMasterSpecification(deployment, additionalResources);
	}

	private static Deployment buildJobManagerDeployment(
			FlinkPod flinkPod,
			KubernetesMasterConf kubernetesMasterConf) {
		final Container resolvedMainContainer = flinkPod.getMainContainer();

		final Pod resolvedPod = new PodBuilder(flinkPod.getPod())
			.editOrNewSpec()
				.addToContainers(resolvedMainContainer)
				.endSpec()
			.build();

		final Map<String, String> labels = resolvedPod.getMetadata().getLabels();

		return new DeploymentBuilder()
			.editOrNewMetadata()
				.withName(kubernetesMasterConf.getClusterId())
				.withLabels(kubernetesMasterConf.getCommonLabels())
				.endMetadata()
			.editOrNewSpec()
				.withReplicas(1)
				.editOrNewTemplate()
					.editOrNewMetadata()
						.withLabels(labels)
						.endMetadata()
					.withSpec(resolvedPod.getSpec())
					.endTemplate()
				.editOrNewSelector()
					.addToMatchLabels(labels)
					.endSelector()
				.endSpec()
			.build();
	}

	public static KubernetesPod buildTaskManagerComponent(KubernetesTaskManagerConf kubernetesTaskManagerConf) {
		FlinkPod flinkPod = new FlinkPodBuilder().build();

		final List<KubernetesStepDecorator> stepDecorators = Arrays.asList(
			new InitTaskManagerDecorator(kubernetesTaskManagerConf),
			new StartCommandDecorator(kubernetesTaskManagerConf),
			new MountVolumesDecorator(kubernetesTaskManagerConf),
			new FlinkConfConfigMapDecorator(kubernetesTaskManagerConf));

		stepDecorators.forEach(step -> step.configureFlinkPod(flinkPod));

		final Pod pod = new PodBuilder(flinkPod.getPod())
			.editOrNewSpec()
				.addToContainers(flinkPod.getMainContainer())
				.endSpec()
			.build();

		return new KubernetesPod(pod);
	}
}
