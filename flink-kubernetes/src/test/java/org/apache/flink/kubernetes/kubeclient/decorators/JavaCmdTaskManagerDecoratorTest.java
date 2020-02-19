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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.taskmanager.KubernetesTaskExecutorRunner;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.test.util.TestBaseUtils;

import io.fabric8.kubernetes.api.model.Container;
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

/**
 * Test for {@link JavaCmdTaskManagerDecorator}.
 */
public class JavaCmdTaskManagerDecoratorTest extends TaskManagerDecoratorTest {

	private static final String _KUBERNETES_ENTRY_PATH = "/opt/flink/bin/start.sh";
	private static final String _INTERNAL_FLINK_CONF_DIR = "/opt/flink/flink-conf-";
	private static final String _INTERNAL_FLINK_LOG_DIR = "/opt/flink/flink-log-";

	private static final String java = "$JAVA_HOME/bin/java";
	private static final String classpath = "-classpath $FLINK_CLASSPATH";
	private static final String jvmOpts = "-Djvm";

	private static final String tmJvmMem =
			"-Xmx251658235 -Xms251658235 -XX:MaxDirectMemorySize=211392922 -XX:MaxMetaspaceSize=100663296";

	private static final String mainClass = KubernetesTaskExecutorRunner.class.getCanonicalName();
	private String mainClassArgs;

	// Logging variables
	private static final String logback =
			String.format("-Dlogback.configurationFile=file:%s/logback.xml", _INTERNAL_FLINK_CONF_DIR);
	private static final String log4j =
			String.format("-Dlog4j.configuration=file:%s/log4j.properties", _INTERNAL_FLINK_CONF_DIR);
	private static final String tmLogfile =
			String.format("-Dlog.file=%s/taskmanager.log", _INTERNAL_FLINK_LOG_DIR);
	private static final String tmLogRedirects = String.format(
			"1> %s/taskmanager.out 2> %s/taskmanager.err",
			_INTERNAL_FLINK_LOG_DIR,
			_INTERNAL_FLINK_LOG_DIR);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private File flinkConfDir;

	private JavaCmdTaskManagerDecorator javaCmdTaskManagerDecorator;

	@Before
	public void setup() throws IOException {
		flinkConfig.setString(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH, _KUBERNETES_ENTRY_PATH);
		flinkConfig.set(KubernetesConfigOptions.FLINK_CONF_DIR, _INTERNAL_FLINK_CONF_DIR);
		flinkConfig.set(KubernetesConfigOptions.FLINK_LOG_DIR, _INTERNAL_FLINK_LOG_DIR);

		super.setup();

		this.mainClassArgs = String.format(
				"%s--configDir %s",
				TaskExecutorProcessUtils.generateDynamicConfigsStr(taskExecutorProcessSpec),
				_INTERNAL_FLINK_CONF_DIR);

		this.flinkConfDir = temporaryFolder.newFolder().getAbsoluteFile();
		Map<String, String> map = new HashMap<>();
		map.put(ConfigConstants.ENV_FLINK_CONF_DIR, flinkConfDir.toString());
		TestBaseUtils.setEnv(map);

		this.javaCmdTaskManagerDecorator = new JavaCmdTaskManagerDecorator(this.kubernetesTaskManagerConf);
	}

	@Test
	public void testWhetherContainerOrPodIsUpdated() {
		final FlinkPod resultFlinkPod = javaCmdTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod);
		assertEquals(this.baseFlinkPod.getPod(), resultFlinkPod.getPod());
		assertNotEquals(this.baseFlinkPod.getMainContainer(), resultFlinkPod.getMainContainer());
	}

	@Test
	public void testStartCommandWithoutLog4jAndLogback() {
		final Container resultMainContainer =
			javaCmdTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod).getMainContainer();
		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedCommand = getTaskManagerExpectedCommand("", "");

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLog4j() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");

		final Container resultMainContainer =
			javaCmdTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedCommand = getTaskManagerExpectedCommand("", log4j);

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		final Container resultMainContainer =
			javaCmdTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedCommand = getTaskManagerExpectedCommand("", logback);

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLog4jAndLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");

		final Container resultMainContainer =
			javaCmdTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod).getMainContainer();
		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedCommand = getTaskManagerExpectedCommand("", logback + " " + log4j);

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLogAndJVMOpts() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		flinkConfig.set(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		final Container resultMainContainer =
				javaCmdTaskManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedCommand =
				getTaskManagerExpectedCommand(jvmOpts, logback + " " + log4j);

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLogAndJMOpts() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		flinkConfig.set(CoreOptions.FLINK_TM_JVM_OPTIONS, jvmOpts);
		final Container resultMainContainer =
				javaCmdTaskManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));


		final String expectedCommand =
				getTaskManagerExpectedCommand(jvmOpts, logback + " " + log4j);

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testContainerStartCommandTemplate1() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		final String containerStartCommandTemplate =
				"%java% 1 %classpath% 2 %jvmmem% %jvmopts% %logging% %class% %args% %redirects%";
		this.flinkConfig.set(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE,
				containerStartCommandTemplate);

		final String tmJvmOpts = "-DjmJvm";
		this.flinkConfig.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		this.flinkConfig.setString(CoreOptions.FLINK_TM_JVM_OPTIONS, tmJvmOpts);

		final Container resultMainContainer =
				javaCmdTaskManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedCommand = java + " 1 " + classpath + " 2 " + tmJvmMem +
				" " + jvmOpts + " " + tmJvmOpts +
				" " + tmLogfile + " " + logback + " " + log4j +
				" " + mainClass + " " + mainClassArgs + " " + tmLogRedirects;

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);

		assertThat(resultMainContainer.getArgs(), is(expectedArgs));
	}

	@Test
	public void testContainerStartCommandTemplate2() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		final String containerStartCommandTemplate =
				"%java% %jvmmem% %logging% %jvmopts% %class% %args% %redirects%";
		this.flinkConfig.set(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE,
				containerStartCommandTemplate);

		final String tmJvmOpts = "-DjmJvm";
		this.flinkConfig.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		this.flinkConfig.setString(CoreOptions.FLINK_TM_JVM_OPTIONS, tmJvmOpts);

		final Container resultMainContainer =
				javaCmdTaskManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedCommand = java + " " + tmJvmMem +
				" " + tmLogfile + " " + logback + " " + log4j +
				" " + jvmOpts + " " + tmJvmOpts + " " + mainClass +
				" " + mainClassArgs + " " + tmLogRedirects;

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);

		assertThat(resultMainContainer.getArgs(), is(expectedArgs));
	}

	private String getTaskManagerExpectedCommand(String jvmAllOpts, String logging) {
		return java + " " + classpath + " " + tmJvmMem +
				(jvmAllOpts.isEmpty() ? "" : " " + jvmAllOpts) +
				(logging.isEmpty() ? "" : " " + tmLogfile + " " + logging) +
				" " + mainClass + " " +  mainClassArgs + " " + tmLogRedirects;
	}
}
