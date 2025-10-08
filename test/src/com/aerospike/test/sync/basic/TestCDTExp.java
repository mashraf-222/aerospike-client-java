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
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.CDTExp;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.LoopVarPart;
import com.aerospike.test.sync.TestSync;

public class TestCDTExp extends TestSync {
    
    private static final String NAMESPACE = "test";
    private static final String SET = "testset";
    
    @Test
    public void testCDTExpSelect() {
        Key keyA = new Key(NAMESPACE, SET, "cdtExpSelectKey");
        
        try {
            client.delete(null, keyA);
        } catch (Exception e) {
        }
        
        List<Map<String, Object>> booksList = new ArrayList<>();
        
        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Sayings of the Century");
        book1.put("price", 10.45);
        booksList.add(book1);
        
        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Sword of Honour");
        book2.put("price", 20.99);
        booksList.add(book2);
        
        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Moby Dick");
        book3.put("price", 5.01);
        booksList.add(book3);
        
        Map<String, Object> book4 = new HashMap<>();
        book4.put("title", "The Lord of the Rings");
        book4.put("price", 30.98);
        booksList.add(book4);
        
        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);
        
        Bin bin = new Bin("res1", rootMap);
        client.put(null, keyA, bin);
        
        CTX bookKey = CTX.mapKey(Value.get("book"));
        CTX allChildren = CTX.allChildren();
        CTX priceKey = CTX.mapKey(Value.get("price"));
        
        Expression selectExp = Exp.build(
            CDTExp.selectByPath(
                Exp.Type.LIST,                    // Return type: list
                LoopVarPart.VALUE.id,            // AS_CDT_SELECT_LEAF_MAP_VALUE equivalent
                Exp.mapBin("res1"),              // Source bin
                bookKey, allChildren, priceKey   // CTX path
            )
        );
        
        Record result = client.operate(null, keyA, 
            ExpOperation.write("A", selectExp, ExpWriteFlags.DEFAULT)
        );
        
        assertTrue("CDT select expression operation should succeed", result != null);
        
        Record finalRecord = client.get(null, keyA);
        List<?> priceList = finalRecord.getList("A");
        assertTrue("Price list should exist", priceList != null);
        assertTrue("Should have 4 prices", priceList.size() == 4);
        
        double firstPrice = ((Number) priceList.get(0)).doubleValue();
        assertTrue("First price should be < 11", firstPrice < 11);
    }
    
    @Test
    public void testCDTExpApply() {
        Key keyA = new Key(NAMESPACE, SET, "cdtExpApplyKey");
        
        try {
            client.delete(null, keyA);
        } catch (Exception e) {
        }
        
        List<Map<String, Object>> booksList = new ArrayList<>();
        
        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Sayings of the Century");
        book1.put("price", 10.45);
        booksList.add(book1);
        
        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Sword of Honour");
        book2.put("price", 20.99);
        booksList.add(book2);
        
        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Moby Dick");
        book3.put("price", 5.01);
        booksList.add(book3);
        
        Map<String, Object> book4 = new HashMap<>();
        book4.put("title", "The Lord of the Rings");
        book4.put("price", 30.98);
        booksList.add(book4);
        
        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);
        
        Bin bin = new Bin("res1", rootMap);
        client.put(null, keyA, bin);
        
        CTX bookKey = CTX.mapKey(Value.get("book"));
        CTX allChildren = CTX.allChildren();
        CTX priceKey = CTX.mapKey(Value.get("price"));
        
        Exp modifyExp = Exp.mul(
            Exp.loopVarFloat(LoopVarPart.VALUE),  // Current price value
            Exp.val(1.50)                         // Multiply by 1.50
        );
        
        Expression applyExp = Exp.build(
            CDTExp.cdtModify(
                Exp.Type.MAP,                     // Return type: map
                0,                                // Flags
                modifyExp,                        // Modify expression
                Exp.mapBin("res1"),              // Source bin
                bookKey, allChildren, priceKey   // CTX path
            )
        );
        
        Record result = client.operate(null, keyA, 
            ExpOperation.write("res1", applyExp, ExpWriteFlags.UPDATE_ONLY)
        );
        
        assertTrue("CDT apply expression operation should succeed", result != null);
        
        Record finalRecord = client.get(null, keyA);
        assertTrue("Final record should exist", finalRecord != null);
        
        Map<?, ?> finalRootMap = (Map<?, ?>) finalRecord.getValue("res1");
        assertTrue("Root map should exist", finalRootMap != null);
        
        List<?> finalBooksList = (List<?>) finalRootMap.get("book");
        assertTrue("Books list should exist", finalBooksList != null && !finalBooksList.isEmpty());
        
        Map<?, ?> firstBook = (Map<?, ?>) finalBooksList.get(0);
        assertTrue("First book should exist", firstBook != null);
        
        Object priceObj = firstBook.get("price");
        assertTrue("Price should exist", priceObj != null);
        
        double finalPrice = ((Number) priceObj).doubleValue();
        assertTrue("Price should be increased (> 11)", finalPrice > 11.0);
        
        double expectedPrice = 10.45 * 1.50;
        assertTrue("Price should be approximately " + expectedPrice, 
                   Math.abs(finalPrice - expectedPrice) < 0.01);
    }
}
