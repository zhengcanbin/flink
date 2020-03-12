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

package org.apache.flink.kubernetes.configuration;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;

/**
 * Kubernetes configuration options that are not meant to be set by the user.
 */
@Internal
public class KubernetesConfigOptionsInternal {

	public static final ConfigOption<String> ENTRY_POINT_CLASS = ConfigOptions
		.key("kubernetes.internal.jobmanager.entrypoint.class")
		.stringType()
		.noDefaultValue()
		.withDescription("The entrypoint class for jobmanager. It will be set in kubernetesClusterDescriptor.");

	public static final ConfigOption<Boolean> RUN_INIT_CONTAINER = ConfigOptions
		.key("kubernetes.internal.enable.init.container")
		.booleanType()
		.defaultValue(false)
		.withDescription("Whether the JobManager/TaskManager should run the init-Container to fetch remote jars/files");

	public static final ConfigOption<List<String>> REMOTE_JAR_DEPENDENCIES = ConfigOptions
		.key("kubernetes.internal.remote.jar.dependencies")
		.stringType()
		.asList()
		.noDefaultValue()
		.withDescription("A semicolon-separated list of the remote jars that the init Container should help download.");

	public static final ConfigOption<List<String>> REMOTE_FILE_DEPENDENCIES = ConfigOptions
		.key("kubernetes.internal.remote.file.dependencies")
		.stringType()
		.asList()
		.noDefaultValue()
		.withDescription("A semicolon-separated list of the remote files that the init Container should help download.");

	/** This class is not meant to be instantiated. */
	private KubernetesConfigOptionsInternal() {}
}
