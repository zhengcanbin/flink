package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.utils.Constants;

import org.apache.flink.shaded.guava18.com.google.common.io.Files;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Mounts the Hadoop configuration from a local configuration directory.
 */
public class HadoopConfMountDecorator extends AbstractKubernetesStepDecorator {

	private static final Logger LOG = LoggerFactory.getLogger(HadoopConfMountDecorator.class);

	private final AbstractKubernetesParameters kubernetesParameters;

	public HadoopConfMountDecorator(AbstractKubernetesParameters kubernetesParameters) {
		this.kubernetesParameters = checkNotNull(kubernetesParameters);
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		final Optional<String> localHadoopConfigurationDirectory =
			this.kubernetesParameters.getLocalHadoopConfigurationDirectory();
		if (!localHadoopConfigurationDirectory.isPresent()) {
			return flinkPod;
		}

		final List<File> hadoopConfigurationFileItems =
			getHadoopConfigurationFileItems(localHadoopConfigurationDirectory.get());
		if (hadoopConfigurationFileItems.isEmpty()) {
			LOG.warn("Found 0 configuration files in directory {}", localHadoopConfigurationDirectory.get());
			return flinkPod;
		}

		final List<KeyToPath> keyToPaths = hadoopConfigurationFileItems.stream()
			.map(file -> new KeyToPathBuilder()
				.withKey(file.getName())
				.withPath(file.getName())
				.build())
			.collect(Collectors.toList());

		final Volume hadoopConfVolume = new VolumeBuilder()
			.withName(Constants.HADOOP_CONF_VOLUME)
			.withNewConfigMap()
				.withName(getHadoopConfConfigMapName(kubernetesParameters.getClusterId()))
				.withItems(keyToPaths)
				.endConfigMap()
			.build();

		final Pod podWithHadoopConf = new PodBuilder(flinkPod.getPod())
			.editOrNewSpec()
				.addNewVolumeLike(hadoopConfVolume)
					.endVolume()
				.endSpec()
			.build();

		final Container containerWithHadoopConf = new ContainerBuilder(flinkPod.getMainContainer())
			.addNewVolumeMount()
				.withName(Constants.HADOOP_CONF_VOLUME)
				.withMountPath(Constants.HADOOP_CONF_DIR_IN_POD)
				.endVolumeMount()
			.addNewEnv()
				.withName(Constants.ENV_HADOOP_CONF_DIR)
				.withValue(Constants.HADOOP_CONF_DIR_IN_POD)
				.endEnv()
			.build();

		return new FlinkPod.Builder(flinkPod)
			.withPod(podWithHadoopConf)
			.withMainContainer(containerWithHadoopConf)
			.build();
	}

	@Override
	public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
		final Optional<String> localHadoopConfigurationDirectory =
			this.kubernetesParameters.getLocalHadoopConfigurationDirectory();
		if (!localHadoopConfigurationDirectory.isPresent()) {
			return Collections.emptyList();
		}

		final List<File> hadoopConfigurationFileItems =
			getHadoopConfigurationFileItems(localHadoopConfigurationDirectory.get());
		if (hadoopConfigurationFileItems.isEmpty()) {
			LOG.warn("Found 0 configuration files in directory {}", localHadoopConfigurationDirectory.get());
			return Collections.emptyList();
		}

		final Map<String, String> data = new HashMap<>();
		for (File file: hadoopConfigurationFileItems) {
			data.put(file.getName(), Files.toString(file, StandardCharsets.UTF_8));
		}

		final ConfigMap hadoopConfigMap = new ConfigMapBuilder()
			.withApiVersion(Constants.API_VERSION)
			.withNewMetadata()
				.withName(getHadoopConfConfigMapName(kubernetesParameters.getClusterId()))
				.withLabels(kubernetesParameters.getCommonLabels())
				.endMetadata()
			.addToData(data)
			.build();

		return Collections.singletonList(hadoopConfigMap);
	}

	private List<File> getHadoopConfigurationFileItems(String localHadoopConfigurationDirectory) {
		final File file = new File(localHadoopConfigurationDirectory);
		if (file.exists() && file.isDirectory()) {
			return Arrays.stream(file.listFiles()).filter(File::isFile).collect(Collectors.toList());
		} else {
			return Collections.emptyList();
		}
	}

	public static String getHadoopConfConfigMapName(String clusterId) {
		return Constants.HADOOP_CONF_CONFIG_MAP_PREFIX + clusterId;
	}
}
