/*
 * Copyright 2012-2025 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.admin.Role;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.test.sync.TestSync;

public class TestQueryRoles extends TestSync {
	@Test
	public void testQueryRolesAll() {
		AdminPolicy policy = new AdminPolicy();
		
		try {
			List<Role> roles = client.queryRoles(policy);
			assertNotNull("Roles list should not be null", roles);
			
			System.out.println("Successfully queried " + roles.size() + " roles");
			for (Role role : roles) {
				System.out.println("  - " + role.name);
			}
			
		} catch (AerospikeException e) {
			if (e.getResultCode() == ResultCode.SECURITY_NOT_ENABLED) {
				System.out.println("Skipping test: Security is not enabled on the server");
				return;
			}
			if (e.getResultCode() == ResultCode.NOT_AUTHENTICATED) {
				System.out.println("Skipping test: Not authenticated - " + e.getMessage());
				return;
			}
			if (e.getResultCode() == ResultCode.SECURITY_NOT_SUPPORTED) {
				System.out.println("Skipping test: Security not supported - " + e.getMessage());
				return;
			}
			throw e;
		}
	}
}
