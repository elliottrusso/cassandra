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

package org.apache.cassandra.index.sai.disk;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

/*
CURRENT STATE OF LIKE OPERATOR:
-> Tests fail due to min and max are set to null and count = 0 in KeyRangeIterator constructor.
-> KeyRangeIterator.skipTo() returns endOfData().
-> Also in Expression.add(), the operators are in the switch so the lower and upper are returned
   as the same. Not sure if this needs to be looked at.
-> The operator functionality seems good in the tests but since the tests aren't passing yet,
   we can't see if it is producing the correct output.
 */

public class LikeOperatorTest extends SAITester
{
    @Test
    public void testStringLikePrefix() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, v text);");
        createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 0, "a");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "abc");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "ac");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "abcd");
        Assert.assertEquals(2, execute("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE v LIKE 'ab%%'").size());
    }

    @Test
    public void testStringLikeSuffix() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, v text);");
        createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 0, "a");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "abc");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "ac");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "abcd");
        Assert.assertEquals(1, execute("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE v LIKE '%%cd'").size());
    }

    @Test
    public void testStringLikeContains() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, v text);");
        createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 0, "a");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "abc");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "ac");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "abcd");
        Assert.assertEquals(2, execute("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE v LIKE '%%bc%%'").size());
    }

    @Test
    public void testStringLikeMatches() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, v text);");
        createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 0, "a");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "abc");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "ac");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "abcd");
        Assert.assertEquals(1, execute("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE v LIKE 'abc'").size());
    }
}
