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
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.builder.FlinkPodBuilder;
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
 * Test for {@link JavaCmdJobManagerDecorator}.
 */
public class JavaCmdJobManagerDecoratorTest extends JobManagerDecoratorTest {

	private static final String _KUBERNETES_ENTRY_PATH = "/opt/bin/start.sh";
	private static final String _INTERNAL_FLINK_CONF_DIR = "/opt/flink/flink-conf-";
	private static final String _INTERNAL_FLINK_LOG_DIR = "/opt/flink/flink-log-";
	private static final String _ENTRY_POINT_CLASS = KubernetesSessionClusterEntrypoint.class.getCanonicalName();

	private static final String java = "$JAVA_HOME/bin/java";
	private static final String classpath = "-classpath $FLINK_CLASSPATH";
	private static final String jvmOpts = "-Djvm";

	// Logging variables
	private static final String logback =
			String.format("-Dlogback.configurationFile=file:%s/logback.xml", _INTERNAL_FLINK_CONF_DIR);
	private static final String log4j =
			String.format("-Dlog4j.configuration=file:%s/log4j.properties", _INTERNAL_FLINK_CONF_DIR);
	private static final String jmLogfile = String.format("-Dlog.file=%s/jobmanager.log", _INTERNAL_FLINK_LOG_DIR);
	private static final String jmLogRedirects =
			String.format("1> %s/jobmanager.out 2> %s/jobmanager.err",
					_INTERNAL_FLINK_LOG_DIR, _INTERNAL_FLINK_LOG_DIR);

	// Memory variables
	private static final String jmJvmMem = String.format("-Xms%dm -Xmx%dm",
			JOB_MANAGER_MEMORY - 600, JOB_MANAGER_MEMORY - 600);

	private final FlinkPod baseFlinkPod = new FlinkPodBuilder().build();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private File flinkConfDir;

	private JavaCmdJobManagerDecorator javaCmdJobManagerDecorator;

	@Before
	public void setup() throws IOException {
		super.setup();

		this.flinkConfDir = temporaryFolder.newFolder().getAbsoluteFile();
		final Map<String, String> map = new HashMap<>();
		map.put(ConfigConstants.ENV_FLINK_CONF_DIR, flinkConfDir.toString());
		TestBaseUtils.setEnv(map);

		flinkConfig.set(KubernetesConfigOptions.FLINK_CONF_DIR, _INTERNAL_FLINK_CONF_DIR);
		flinkConfig.set(KubernetesConfigOptions.FLINK_LOG_DIR, _INTERNAL_FLINK_LOG_DIR);
		flinkConfig.set(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, _ENTRY_POINT_CLASS);
		flinkConfig.set(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH, _KUBERNETES_ENTRY_PATH);

		this.javaCmdJobManagerDecorator = new JavaCmdJobManagerDecorator(kubernetesMasterConf);
	}

	@Test
	public void testWhetherContainerOrPodIsReplaced() {
		final FlinkPod resultFlinkPod = javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod);
		assertEquals(baseFlinkPod.getPod(), resultFlinkPod.getPod());
		assertNotEquals(baseFlinkPod.getMainContainer(), resultFlinkPod.getMainContainer());
	}

	@Test
	public void testStartCommandWithoutLog4jAndLogback() {
		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedCommand = getJobManagerExpectedCommand("", "");

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);

		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLog4j() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");

		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedCommand = getJobManagerExpectedCommand("", log4j);
		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedCommand = getJobManagerExpectedCommand("", logback);
		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLog4jAndLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedCommand =
				getJobManagerExpectedCommand("", logback + " " + log4j);

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLogAndJVMOpts() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		flinkConfig.set(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));


		final String expectedCommand =
				getJobManagerExpectedCommand(jvmOpts, logback + " " + log4j);

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLogAndJMOpts() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		flinkConfig.set(CoreOptions.FLINK_JM_JVM_OPTIONS, jvmOpts);
		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));


		final String expectedCommand =
				getJobManagerExpectedCommand(jvmOpts, logback + " " + log4j);

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

		final String jmJvmOpts = "-DjmJvm";
		this.flinkConfig.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		this.flinkConfig.setString(CoreOptions.FLINK_JM_JVM_OPTIONS, jmJvmOpts);

		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedCommand = java + " 1 " + classpath + " 2 " + jmJvmMem +
				" " + jvmOpts + " " + jmJvmOpts +
				" " + jmLogfile + " " + logback + " " + log4j +
				" " + _ENTRY_POINT_CLASS + " " + jmLogRedirects;

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

		final String jmJvmOpts = "-DjmJvm";
		this.flinkConfig.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		this.flinkConfig.setString(CoreOptions.FLINK_JM_JVM_OPTIONS, jmJvmOpts);

		final Container resultMainContainer =
				javaCmdJobManagerDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedCommand = java + " " + jmJvmMem +
				" " + jmLogfile + " " + logback + " " + log4j +
				" " + jvmOpts + " " + jmJvmOpts +
				" " + _ENTRY_POINT_CLASS + " " + jmLogRedirects;

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);

		assertThat(resultMainContainer.getArgs(), is(expectedArgs));
	}

	private String getJobManagerExpectedCommand(String jvmAllOpts, String logging) {
		return java + " " + classpath + " " + jmJvmMem +
				(jvmAllOpts.isEmpty() ? "" : " " + jvmAllOpts) +
				(logging.isEmpty() ? "" : " " + jmLogfile + " " + logging) +
				" " + _ENTRY_POINT_CLASS + " " + jmLogRedirects;
	}
}