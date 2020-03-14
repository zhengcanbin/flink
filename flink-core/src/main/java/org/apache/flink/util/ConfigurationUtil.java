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

package org.apache.flink.util;

import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for {@link org.apache.flink.configuration.Configuration}.
 */
public class ConfigurationUtil {

	/**
	 * Extract and parse Flink configuration properties with a given name prefix and
	 * return the result as a Map. Keys must not have more than one value.
	 */
	public static Map<String, String> getPrefixedKeyValuePairs(Configuration configuration, String prefix) {
		Map<String, String> result  = new HashMap<>();
		for (Map.Entry<String, String> entry: configuration.toMap().entrySet()) {
			if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
				String key = entry.getKey().substring(prefix.length());
				result.put(key, entry.getValue());
			}
		}
		return result;
	}

}
