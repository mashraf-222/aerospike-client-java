package com.aerospike.client.exp;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.operation.SelectFlags;
import com.aerospike.client.util.Packer;

public class CDTExp {
	private static final int MODULE = 0;
	public static final int MODIFY = 0x40;

    enum Type {
        SELECT(0xfe),
        MODIFY(0xff);

        int value;

        Type(int value) {
            this.value = value;
        }
    }

    public static Exp cdtSelect(Exp.Type returnType, int flags, Exp bin, CTX... ctx) {
        byte[] bytes = packCdtSelect(Type.SELECT, flags, ctx);

        return new Exp.Module(bin, bytes, returnType.code, MODULE);
    }

    public static Exp cdtModify(Exp.Type returnType, int flags, Exp modifyExp, Exp bin, CTX... ctx) {
        byte[] bytes = packCdtModify(Type.SELECT, flags | 4, modifyExp, ctx);

        return new Exp.Module(bin, bytes, returnType.code, MODULE | MODIFY);
    }

	private static byte[] packCdtModify(Type type, int selectFlag, Exp modifyExp, CTX... ctx) {
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

            packer.packInt(selectFlag);
            modifyExp.pack(packer);

            if (i == 0) {
                packer.createBuffer();
            }

        }

        return packer.getBuffer();
	}

	private static byte[] packCdtSelect(Type type, int selectFlags, CTX... ctx) {
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

            packer.packInt(selectFlags);

            if (i == 0) {
                packer.createBuffer();
            }

        }

        return packer.getBuffer();
	}
}
