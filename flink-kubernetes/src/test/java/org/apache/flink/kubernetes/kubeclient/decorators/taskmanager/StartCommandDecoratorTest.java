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

package org.apache.flink.kubernetes.kubeclient.decorators.taskmanager;

import io.fabric8.kubernetes.api.model.Container;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.taskmanager.KubernetesTaskExecutorRunner;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class StartCommandDecoratorTest extends TaskManagerDecoratorTest {

	private static final String _KUBERNETES_ENTRY_PATH = "/opt/flink/bin/start.sh";
	private static final String _INTERNAL_FLINK_CONF_DIR = "/opt/flink/flink-conf-";
	private static final String _INTERNAL_FLINK_LOG_DIR = "/opt/flink/flink-log-";

	private final String java = "$JAVA_HOME/bin/java";
	private final String classpath = "-classpath $FLINK_CLASSPATH";
	private final String jvmmemOpts = TaskExecutorProcessUtils.generateJvmParametersStr(this.taskExecutorProcessSpec);
	private final String mainClass = KubernetesTaskExecutorRunner.class.getCanonicalName();
	private final String mainClassArgs = String.format(
		"%s--configDir %s",
		TaskExecutorProcessUtils.generateDynamicConfigsStr(taskExecutorProcessSpec),
		_INTERNAL_FLINK_CONF_DIR);
	private final String redirects = String.format(
		"1> %s/taskmanager.out 2> %s/taskmanager.err",
		_INTERNAL_FLINK_LOG_DIR,
		_INTERNAL_FLINK_LOG_DIR);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private File flinkConfDir;
	private StartCommandDecorator startCommandDecorator;

	@Before
	public void setup() throws IOException {
		super.setup();
		flinkConfig.setString(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH, _KUBERNETES_ENTRY_PATH);
		flinkConfig.set(KubernetesConfigOptions.FLINK_CONF_DIR, _INTERNAL_FLINK_CONF_DIR);
		flinkConfig.set(KubernetesConfigOptions.FLINK_LOG_DIR, _INTERNAL_FLINK_LOG_DIR);

		this.flinkConfDir = temporaryFolder.newFolder().getAbsoluteFile();
		Map<String, String> map = new HashMap<>();
		map.put(ConfigConstants.ENV_FLINK_CONF_DIR, flinkConfDir.toString());
		TestBaseUtils.setEnv(map);

		this.startCommandDecorator = new StartCommandDecorator(this.kubernetesTaskManagerConf);
	}

	@Test
	public void testWhetherContainerOrPodIsUpdated() {
		final FlinkPod resultFlinkPod = startCommandDecorator.configureFlinkPod(this.baseFlinkPod);
		assertEquals(this.baseFlinkPod.getPod(), resultFlinkPod.getPod());
		assertNotEquals(this.baseFlinkPod.getMainContainer(), resultFlinkPod.getMainContainer());
	}

	@Test
	public void testStartCommandWithoutLog4jAndLogback() {
		final Container resultMainContainer =
			startCommandDecorator.configureFlinkPod(this.baseFlinkPod).getMainContainer();
		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedCommand = String.format(
			"%s %s %s %s %s %s",
			this.java,
			this.classpath,
			this.jvmmemOpts,
			this.mainClass,
			this.mainClassArgs,
			this.redirects);

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLog4j() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");

		final Container resultMainContainer =
			startCommandDecorator.configureFlinkPod(this.baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String logging = String.format(
			"-Dlog.file=%s/taskmanager.log -Dlog4j.configuration=file:%s/log4j.properties",
			_INTERNAL_FLINK_LOG_DIR,
			_INTERNAL_FLINK_CONF_DIR);

		final String expectedCommand = String.format(
			"%s %s %s %s %s %s %s",
			this.java,
			this.classpath,
			this.jvmmemOpts,
			logging,
			this.mainClass,
			this.mainClassArgs,
			this.redirects);

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		final Container resultMainContainer =
			startCommandDecorator.configureFlinkPod(this.baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String logging = String.format(
			"-Dlog.file=%s/taskmanager.log -Dlogback.configurationFile=file:%s/logback.xml",
			_INTERNAL_FLINK_LOG_DIR,
			_INTERNAL_FLINK_CONF_DIR);

		final String expectedCommand = String.format(
			"%s %s %s %s %s %s %s",
			this.java,
			this.classpath,
			this.jvmmemOpts,
			logging,
			this.mainClass,
			this.mainClassArgs,
			this.redirects);

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLog4jAndLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");

		final Container resultMainContainer =
			startCommandDecorator.configureFlinkPod(this.baseFlinkPod).getMainContainer();
		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String logging = String.format(
			"-Dlog.file=%s/taskmanager.log -Dlogback.configurationFile=file:%s/logback.xml" +
				" -Dlog4j.configuration=file:%s/log4j.properties",
			_INTERNAL_FLINK_LOG_DIR,
			_INTERNAL_FLINK_CONF_DIR,
			_INTERNAL_FLINK_CONF_DIR);

		final String expectedCommand = String.format(
			"%s %s %s %s %s %s %s",
			this.java,
			this.classpath,
			this.jvmmemOpts,
			logging,
			this.mainClass,
			this.mainClassArgs,
			this.redirects);

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}
}
