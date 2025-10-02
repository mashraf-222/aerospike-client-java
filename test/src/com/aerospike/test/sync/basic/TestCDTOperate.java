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

package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CDTOperation;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.LoopVarPart;
import com.aerospike.client.exp.MapExp;
import com.aerospike.test.sync.TestSync;

public class TestCDTOperate extends TestSync {
    
    private static final String NAMESPACE = "test";
    private static final String SET = "testset";
    private static final String BIN_NAME = "testbin";
    
    @Test
    public void testCDTOperateWithExpressions() {
        Key rkey = new Key(NAMESPACE, SET, 215);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        List<Map<String, Object>> booksList = new ArrayList<>();
        
        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Sayings of the Century");
        book1.put("price", 8.95);
        booksList.add(book1);
        
        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Sword of Honour");
        book2.put("price", 12.99);
        booksList.add(book2);
        
        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Moby Dick");
        book3.put("price", 8.99);
        booksList.add(book3);
        
        Map<String, Object> book4 = new HashMap<>();
        book4.put("title", "The Lord of the Rings");
        book4.put("price", 22.99);
        booksList.add(book4);
        
        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);
        
        Bin bin = new Bin(BIN_NAME, rootMap);
        client.put(null, rkey, bin);
        
        Record record = client.get(null, rkey);
        assertTrue("Record should exist", record != null);
        
        CTX ctx1 = CTX.allChildren();
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.le(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT, 
                    Exp.val("price"), Exp.loopVarMap(LoopVarPart.VALUE)),
                Exp.val(10.0)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.loopVarString(LoopVarPart.MAP_KEY), Exp.val("title"))
        );
        
        Operation selectOp = CDTOperation.cdtSelect(BIN_NAME, LoopVarPart.VALUE.id, ctx1, ctx2, ctx3);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue("CDT select operation should succeed", result != null);
        
        List<?> results = result.getList(BIN_NAME);
        if (results != null && !results.isEmpty()) {
            System.out.println("Selected titles: " + results);
        }
    }
    
    @Test
    public void testCDTApplyWithExpressions() {
        Key rkey = new Key(NAMESPACE, SET, 216);
        
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }
        
        List<Map<String, Object>> booksList = new ArrayList<>();
        
        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Sayings of the Century");
        book1.put("price", 8.95);
        booksList.add(book1);
        
        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Sword of Honour");
        book2.put("price", 12.99);
        booksList.add(book2);
        
        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Moby Dick");
        book3.put("price", 8.99);
        booksList.add(book3);
        
        Map<String, Object> book4 = new HashMap<>();
        book4.put("title", "The Lord of the Rings");
        book4.put("price", 22.99);
        booksList.add(book4);
        
        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);
        
        Bin bin = new Bin(BIN_NAME, rootMap);
        client.put(null, rkey, bin);
        
        Record record = client.get(null, rkey);
        assertTrue("Record should exist", record != null);
        
        CTX bookKey = CTX.mapKey(Value.get("book"));
        CTX allChildren = CTX.allChildren();
        CTX priceKey = CTX.mapKey(Value.get("price"));
        
        Expression modifyExp = Exp.build(
            Exp.mul(
                Exp.loopVarFloat(LoopVarPart.VALUE),  // Current price value
                Exp.val(1.10)                         // Multiply by 1.10
            )
        );
        
        Operation applyOp = CDTOperation.cdtApply(BIN_NAME, 0, modifyExp, bookKey, allChildren, priceKey);
        
        Record result = client.operate(null, rkey, applyOp);
        assertTrue("CDT apply operation should succeed", result != null);
        
        Record finalRecord = client.get(null, rkey);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalRootMap = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue("Root map should exist", finalRootMap != null);
        
        List<?> finalBooksList = (List<?>) finalRootMap.get("book");
        assertTrue("Books list should exist", finalBooksList != null && !finalBooksList.isEmpty());
        
        Map<?, ?> firstBook = (Map<?, ?>) finalBooksList.get(0);
        assertTrue("First book should exist", firstBook != null);
        
        Object priceObj = firstBook.get("price");
        assertTrue("Price should exist", priceObj != null);
        
        double finalPrice = ((Number) priceObj).doubleValue();
        assertTrue("Price should be increased (> 9)", finalPrice > 9.0);
        
        double expectedPrice = 8.95 * 1.10;
        assertTrue("Price should be approximately " + expectedPrice, 
                   Math.abs(finalPrice - expectedPrice) < 0.01);
    }
}
