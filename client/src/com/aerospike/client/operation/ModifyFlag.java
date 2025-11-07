package com.aerospike.client.operation;

/*8
 * Flags that control what data is modified by path expression operations.
 */
public enum ModifyFlag {
    /**
	* If the expression in the context hits an invalid type, the operation
	* will fail.  This is the default behavior.
    */
	DEFAULT(0x00),

	// This flag is set when leaf values are to be modified.
	APPLY(0x04),

	// If the expression in the context hits an invalid type (e.g., selects
	// as an integer when the value is a string), do not fail the operation;
	// just ignore those elements.  Interpret UNKNOWN as false instead.
	NO_FAIL(0x10);

	public final int flag;

	private ModifyFlag(int flag) {
		this.flag = flag;
	}
}