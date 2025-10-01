package com.aerospike.client.cdt;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.operation.SelectFlags;
import com.aerospike.client.util.Packer;
import com.aerospike.client.exp.Expression;

public class CDTOperation {
    enum Type {
        SELECT(0xfe),
        MODIFY(0xff);

        int value;

        Type(int value) {
            this.value = value;
        }
    }
	
    /**
     * Create CDT select operation with context.
     * Equivalent to as_operations_cdt_select in C client.
     *
     * @param binName		bin name
     * @param flags			select flags
     * @param ctx			optional path to nested CDT. If not defined, the top-level CDT is used.
     */
    public static Operation cdtSelect(String binName, int flags, CTX... ctx) {
        if (ctx == null) {
            return null;
        }

        byte[] packedBytes = packCdtSelect(flags, Type.SELECT, ctx);
        return new Operation(Operation.Type.CDT_READ, binName, Value.get(packedBytes));
    }

    /**
     * Create CDT apply operation with context and modify expression.
     * Equivalent to as_operations_cdt_apply in C client.
     *
     * @param binName		bin name
     * @param flags			select flags
     * @param modifyExp		modify expression
     * @param ctx			optional path to nested CDT. If not defined, the top-level CDT is used.
     */
    public static Operation cdtApply(String binName, int flags, Expression modifyExp, CTX... ctx) {
        if (ctx == null) {
            return null;
        }

        byte[] packedBytes = packCdtApply(flags | 4, Type.SELECT, modifyExp, ctx);
        return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(packedBytes));
    }
	
	private static byte[] packCdtSelect(SelectFlags selectFlags, CDTOperation.Type type, CTX... ctx) {
        Packer packer = new Packer();

        for (int i = 0; i < 2; i++) {
            packer.packArrayBegin(3);
            packer.packInt(type.value);
            packer.packArrayBegin(ctx.length * 2);

            for (CTX c : ctx) {
                packer.packInt(c.id);
                if (c.value != null)
                    c.value.pack(packer);
                else 
                    packer.packByteArray(c.exp.getBytes(), 0, c.exp.getBytes().length);
            }

            packer.packInt(selectFlags.flag);

            if (i == 0) {
                packer.createBuffer();
            }
        }

        return packer.getBuffer();
	}

	private static byte[] packCdtSelect(int flags, CDTOperation.Type type, CTX... ctx) {
        Packer packer = new Packer();

        for (int i = 0; i < 2; i++) {
            packer.packArrayBegin(3);
            packer.packInt(type.value);
            packer.packArrayBegin(ctx.length * 2);

            for (CTX c : ctx) {
                packer.packInt(c.id);
                if (c.value != null)
                    c.value.pack(packer);
                else 
                    packer.packByteArray(c.exp.getBytes(), 0, c.exp.getBytes().length);
            }

            packer.packInt(flags);

            if (i == 0) {
                packer.createBuffer();
            }
        }

        return packer.getBuffer();
	}

	private static byte[] packCdtApply(int flags, CDTOperation.Type type, Expression modifyExp, CTX... ctx) {
        Packer packer = new Packer();

        for (int i = 0; i < 2; i++) {
            packer.packArrayBegin(4);
            packer.packInt(type.value);
            packer.packArrayBegin(ctx.length * 2);

            for (CTX c : ctx) {
                packer.packInt(c.id);
                if (c.value != null)
                    c.value.pack(packer);
                else 
                    packer.packByteArray(c.exp.getBytes(), 0, c.exp.getBytes().length);
            }

            packer.packInt(flags);
            packer.packByteArray(modifyExp.getBytes(), 0, modifyExp.getBytes().length);

            if (i == 0) {
                packer.createBuffer();
            }
        }

        return packer.getBuffer();
	}
}
