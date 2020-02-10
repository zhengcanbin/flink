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

package org.apache.flink.kubernetes.kubeclient.decorators.jobmanager;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.builder.FlinkPodBuilder;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.fabric8.kubernetes.api.model.Container;

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

public class StartCommandMasterDecoratorTest extends JobManagerDecoratorTest {

	private static final String _KUBERNETES_ENTRY_PATH = "/opt/bin/start.sh";
	private static final String _INTERNAL_FLINK_CONF_DIR = "/opt/flink/flink-conf-";
	private static final String _INTERNAL_FLINK_LOG_DIR = "/opt/flink/flink-log-";
	private static final String _ENTRY_POINT_CLASS = KubernetesSessionClusterEntrypoint.class.getCanonicalName();

	private final String java = "$JAVA_HOME/bin/java";
	private final String classpath = "-classpath $FLINK_CLASSPATH";
	private final String jvmmem = String.format(
		"-Xms%sm -Xmx%sm",
		JOB_MANAGER_MEMORY - 600,
		JOB_MANAGER_MEMORY - 600);
	private final String redirects = String.format(
		"1> %s/jobmanager.out 2> %s/jobmanager.err",
			_INTERNAL_FLINK_LOG_DIR,
			_INTERNAL_FLINK_LOG_DIR);

	private final FlinkPod baseFlinkPod = new FlinkPodBuilder().build();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private File flinkConfDir;

	private StartCommandMasterDecorator startCommandMasterDecorator;

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

		this.startCommandMasterDecorator = new StartCommandMasterDecorator(kubernetesMasterConf);
	}

	@Test
	public void testWhetherContainerOrPodIsReplaced() {
		final FlinkPod resultFlinkPod = startCommandMasterDecorator.decorateFlinkPod(baseFlinkPod);
		assertEquals(baseFlinkPod.getPod(), resultFlinkPod.getPod());
		assertNotEquals(baseFlinkPod.getMainContainer(), resultFlinkPod.getMainContainer());
	}

	@Test
	public void testStartCommandWithoutLog4jAndLogback() {
		final Container resultMainContainer =
				startCommandMasterDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedCommand = String.format(
			"%s %s %s %s %s",
			java,
			classpath,
			jvmmem,
			_ENTRY_POINT_CLASS,
			redirects);

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);

		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLog4j() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");

		final Container resultMainContainer =
			startCommandMasterDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedLogging = String.format(
			"-Dlog.file=%s/jobmanager.log -Dlog4j.configuration=file:%s/log4j.properties",
				_INTERNAL_FLINK_LOG_DIR,
				_INTERNAL_FLINK_CONF_DIR);

		final String expectedCommand = String.format(
			"%s %s %s %s %s %s",
			java,
			classpath,
			jvmmem,
			expectedLogging,
			_ENTRY_POINT_CLASS,
			redirects);

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		final Container resultMainContainer =
			startCommandMasterDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedLogging = String.format(
			"-Dlog.file=%s/jobmanager.log -Dlogback.configurationFile=file:%s/logback.xml",
				_INTERNAL_FLINK_LOG_DIR,
				_INTERNAL_FLINK_CONF_DIR);

		final String expectedCommand = String.format(
			"%s %s %s %s %s %s",
			java,
			classpath,
			jvmmem,
			expectedLogging,
				_ENTRY_POINT_CLASS,
			redirects);

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

	@Test
	public void testStartCommandWithLog4jAndLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		final Container resultMainContainer =
			startCommandMasterDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();

		assertThat(Collections.singletonList(_KUBERNETES_ENTRY_PATH), is(resultMainContainer.getCommand()));

		final String expectedLogging = String.format(
			"-Dlog.file=%s/jobmanager.log -Dlogback.configurationFile=file:%s/logback.xml" +
				" -Dlog4j.configuration=file:%s/log4j.properties",
				_INTERNAL_FLINK_LOG_DIR,
				_INTERNAL_FLINK_CONF_DIR,
				_INTERNAL_FLINK_CONF_DIR);

		final String expectedCommand = String.format(
			"%s %s %s %s %s %s",
			java,
			classpath,
			jvmmem,
			expectedLogging,
				_ENTRY_POINT_CLASS,
			redirects);

		final List<String> expectedArgs = Arrays.asList("/bin/bash", "-c", expectedCommand);
		assertThat(expectedArgs, is(resultMainContainer.getArgs()));
	}

//	@Test
//	public void testJobManagerStartCommand() {
//		final Configuration cfg = new Configuration();
//
//		final String jmJvmOpts = "-DjmJvm";
//
//		assertEquals(
//				getJobManagerExpectedCommand("", "", mainClassArgs),
//				getJobManagerStartCommand(cfg, false, false, mainClassArgs));
//
//		// logback only
//		assertEquals(
//				getJobManagerExpectedCommand("", logback, mainClassArgs),
//				getJobManagerStartCommand(cfg, true, false, mainClassArgs));
//
//		// log4j only
//		assertEquals(
//				getJobManagerExpectedCommand("", log4j, mainClassArgs),
//				getJobManagerStartCommand(cfg, false, true, mainClassArgs));
//
//		// logback + log4j
//		assertEquals(
//				getJobManagerExpectedCommand("", logback + " " + log4j, mainClassArgs),
//				getJobManagerStartCommand(cfg, true, true, mainClassArgs));
//
//		// logback + log4j, different JVM opts
//		cfg.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
//		assertEquals(
//				getJobManagerExpectedCommand(jvmOpts, logback + " " + log4j, mainClassArgs),
//				getJobManagerStartCommand(cfg, true, true, mainClassArgs));
//
//		// logback + log4j,different TM JVM opts
//		cfg.setString(CoreOptions.FLINK_JM_JVM_OPTIONS, jmJvmOpts);
//		assertEquals(
//				getJobManagerExpectedCommand(jvmOpts + " " + jmJvmOpts, logback + " " + log4j, mainClassArgs),
//				getJobManagerStartCommand(cfg, true, true, mainClassArgs));
//
//		// no args
//		assertEquals(
//				getJobManagerExpectedCommand(jvmOpts + " " + jmJvmOpts, logback + " " + log4j, ""),
//				getJobManagerStartCommand(cfg, true, true, ""));
//
//		// now try some configurations with different container-start-command-template
//
//		cfg.setString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE,
//				"%java% 1 %classpath% 2 %jvmmem% %jvmopts% %logging% %class% %args% %redirects%");
//		assertEquals(
//				java + " 1 " + classpath + " 2 " + jmJvmMem +
//						" " + jvmOpts + " " + jmJvmOpts + // jvmOpts
//						" " + jmLogfile + " " + logback + " " + log4j +
//						" " + mainClass + " " + mainClassArgs + " " + jmLogRedirects,
//				getJobManagerStartCommand(cfg, true, true, mainClassArgs));
//
//		cfg.setString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE,
//				"%java% %jvmmem% %logging% %jvmopts% %class% %args% %redirects%");
//		assertEquals(
//				java + " " + jmJvmMem +
//						" " + jmLogfile + " " + logback + " " + log4j +
//						" " + jvmOpts + " " + jmJvmOpts + // jvmOpts
//						" " + mainClass + " " + mainClassArgs + " " + jmLogRedirects,
//				getJobManagerStartCommand(cfg, true, true, mainClassArgs));
//
//	}
//
//	private String getJobManagerExpectedCommand(String jvmAllOpts, String logging, String mainClassArgs) {
//		return java + " " + classpath + " " + jmJvmMem +
//				(jvmAllOpts.isEmpty() ? "" : " " + jvmAllOpts) +
//				(logging.isEmpty() ? "" : " " + jmLogfile + " " + logging) +
//				" " + mainClass + (mainClassArgs.isEmpty() ? "" : " " + mainClassArgs) + " " + jmLogRedirects;
//	}
//
//	private String getJobManagerStartCommand(
//			Configuration cfg,
//			boolean hasLogBack,
//			boolean hasLog4j,
//			String mainClassArgs) {
//		return StartCommandMasterDecorator.getJobManagerStartCommand(
//				cfg,
//				jobManagerMem,
//				confDirInPod,
//				logDirInPod,
//				hasLogBack,
//				hasLog4j,
//				mainClass,
//				mainClassArgs
//		);
}
