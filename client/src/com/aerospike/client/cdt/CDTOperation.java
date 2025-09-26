package com.aerospike.client.cdt;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.operation.SelectFlags;
import com.aerospike.client.util.Packer;

public class CDTOperation {
    enum Type {
        SELECT(0xfe),
        MODIFY(0xff);

        int value;

        Type(int value) {
            this.value = value;
        }
    }
	
    public static Operation pathExpression(String binName, SelectFlags returnType, CTX... ctx) {
        byte[] packedBytes = packCdtSelect(returnType, Type.SELECT, ctx);

        return new Operation(Operation.Type.CDT_READ, binName, Value.get(packedBytes));
    }
	
	private static byte[] packCdtSelect(SelectFlags selectFlags, CDTOperation.Type type, CTX... ctx) {
        Packer packer = new Packer();
        packer.packArrayBegin(3);
		packer.packInt(type.value);
		packer.packArrayBegin(ctx.length * 2);

        for (CTX c : ctx) {
            packer.packInt(c.id);
            c.value.pack(packer);
        }

		packer.packArrayBegin(1);
        packer.packInt(selectFlags.flag);

        packer.createBuffer();

        packer.packArrayBegin(3);
		packer.packInt(type.value);
		packer.packArrayBegin(ctx.length * 2);

        for (CTX c : ctx) {
            packer.packInt(c.id);
            c.value.pack(packer);
        }

		packer.packArrayBegin(1);
        packer.packInt(selectFlags.flag);

        return packer.getBuffer();
	}
}
