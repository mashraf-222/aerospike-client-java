/*
 * Copyright 2012-2025 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.util;

import java.net.InetSocketAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;

public final class Version implements Comparable<Version> {
	public static final Version SERVER_VERSION_8_1 = new Version(8, 1, 0, 0);

	public static Version getServerVersion(IAerospikeClient client, InfoPolicy policy) {
		Node node = client.getCluster().getRandomNode();
		return getServerVersion(policy, node);
	}

	public static Version getServerVersion(InfoPolicy policy, Node node) {
		String response = Info.request(policy, node, "build");
		return Version.convertStringToVersion(response);
	}

	private final int major;
	private final int minor;
	private final int patch;
	private final int build;

	public static Version convertStringToVersion(String strVersion, String nodeName, InetSocketAddress primaryAddress) {
		Version version = convertStringToVersion(strVersion);
		if (version == null) {
			throw new AerospikeException("Node " + nodeName + " " + primaryAddress.toString() + " version is invalid: " + strVersion);
		}
		return version;
	}

	public Version(int major, int minor, int patch, int build) {
		this.major = major;
		this.minor = minor;
		this.patch = patch;
		this.build = build;
	}

	public static Version convertStringToVersion(String version) {
		Pattern pattern = Pattern.compile("^(?<major>\\d+)(?:\\.(?<minor>\\d+))?(?:\\.(?<patch>\\d+))?(?:\\.(?<build>\\d+))?(?:[-_\\.~]*?(?<suffix>.+))?$");
		Matcher matcher = pattern.matcher(version);
		if (!matcher.matches()) {
			return null;
		}

		int major, minor, patch, build;

		try {
			major = Integer.parseInt(matcher.group("major"));
			minor = Integer.parseInt(matcher.group("minor"));
			patch = Integer.parseInt(matcher.group("patch"));
			build = Integer.parseInt(matcher.group("build"));
		} catch (NumberFormatException ex) {
			return null;
		}

		return new Version(major, minor, patch, build);
	}

	@Override
    public int compareTo(Version other) {
        if (this.major != other.major) {
			return Integer.compare(this.major, other.major);
		}
        if (this.minor != other.minor) {
			return Integer.compare(this.minor, other.minor);
		} 
        if (this.patch != other.patch) {
			return Integer.compare(this.patch, other.patch);
		}

        return Integer.compare(this.build, other.build);
    }

    public boolean isGreaterOrEqual(Version otherVersion) {
        return this.compareTo(otherVersion) >= 0;
    }

    public boolean isLessThan(Version otherVersion) {
        return this.compareTo(otherVersion) < 0;
    }

    public boolean isGreaterOrEqual(int major, int minor, int patch, int build) {
        return this.compareTo(new Version(major, minor, patch, build)) >= 0;
    }

    public boolean isLessThan(int major, int minor, int patch, int build) {
        return this.compareTo(new Version(major, minor, patch, build)) < 0;
    }

	@Override
	public String toString() {
		return major + "." + minor + "." + patch + "." + build;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Version other = (Version) obj;
		return major == other.major && minor == other.minor && patch == other.patch;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + major;
		result = prime * result + minor;
		result = prime * result + patch;
		return result;
	}
}
