/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.client.operation;

/**
 * Flags that control what data is selected and returned by path expression operations.
 * These flags can be combined using bitwise OR operations.
 */

public enum SelectFlag {
	/**
	 * Return a tree from the root (bin) level to the bottom of the tree,
	 * with only non-filtered out nodes.
	 */
	MATCHING_TREE(0),

	/**
	 * Return the list of the values of the nodes finally selected by the context.
	 * For maps, this returns the value of each (key, value) pair.
	 */
	VALUE(1),

	/**
	 * Return the list of the values of the nodes finally selected by the context.
	 * This is a synonym for VALUE to make it clear in your
	 * source code that you're expecting a list.
	 */
	LIST_VALUE(1),

	/** 
	 * 
	 * Return the list of map values of the nodes finally selected by the context.
	 * This is a synonym for VALUE to make it clear in your
	 * source code that you're expecting a map.  See also MAP_KEY_VALUE.
	 */
	MAP_VALUE(1),

	/**
	 * Return the list of map keys of the nodes finally selected by the context.
	 */
	MAP_KEY(2),

	/**
	 * Returns the list of map (key, value) pairs of the nodes finally selected
	 * by the context.  This is a synonym for setting both
	 * MAP_KEY and MAP_VALUE bits together.
	 */
	MAP_KEY_VALUE(MAP_KEY.flag | MAP_VALUE.flag),

	/**
	 * If the expression in the context hits an invalid type (e.g., selects
	 * as an integer when the value is a string), do not fail the operation;
	 * just ignore those elements.  Interpret UNKNOWN as false instead.
	 */
	NO_FAIL(0x10);

	public final int flag;

	private SelectFlag(int flag) {
		this.flag = flag;
	}
}
