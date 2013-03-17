/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.service;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.junit.BeforeClass;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CassandraServerCql3Test extends SchemaLoader
{    
	@BeforeClass
	public static void setupClass() throws Exception
	{
        Schema.instance.clear(); // Schema are now written on disk and will be reloaded
        new EmbeddedCassandraService().start();
        ThriftSessionManager.instance.setCurrentSocket(new InetSocketAddress(9160));
	}	
	
    @Test
    public void simple_select() throws Exception
    {
        CassandraServer server = new CassandraServer();
        server.set_keyspace("Keyspace1");
        
        server.execute_cql3_query(ByteBufferUtil.bytes("CREATE TABLE test (id text PRIMARY KEY, num int)"), Compression.NONE, ConsistencyLevel.QUORUM);
        
        CqlPreparedResult selectStatement = server.prepare_cql3_query(ByteBufferUtil.bytes("SELECT * FROM test"), Compression.NONE);
        
        List<ByteBuffer> bindVariables = new LinkedList<ByteBuffer>();
        
		CqlResult result = server.execute_prepared_cql3_query(selectStatement.itemId, bindVariables , ConsistencyLevel.QUORUM);
		
		assert result.getRows().size() == 0 : "the result should be empty";

        server.execute_cql3_query(ByteBufferUtil.bytes("INSERT INTO test (id, num) VALUES ('someKey', 123)"), Compression.NONE, ConsistencyLevel.QUORUM);
        
		result = server.execute_prepared_cql3_query(selectStatement.itemId, bindVariables , ConsistencyLevel.QUORUM);
		
		assert result.getRows().size() == 1 : "the result should contain one row";

        server.execute_cql3_query(ByteBufferUtil.bytes("DROP TABLE test"), Compression.NONE, ConsistencyLevel.QUORUM);
    }

    @Test
    public void select_parallel() throws Exception
    {
        CassandraServer server = new CassandraServer();
        
        server.set_keyspace("Keyspace1");
        server.execute_cql3_query(ByteBufferUtil.bytes("CREATE TABLE test (id text PRIMARY KEY, num int)"), Compression.NONE, ConsistencyLevel.QUORUM);
        server.set_keyspace("Keyspace2");
        server.execute_cql3_query(ByteBufferUtil.bytes("CREATE TABLE test (id text PRIMARY KEY, num int)"), Compression.NONE, ConsistencyLevel.QUORUM);

        server.set_keyspace("Keyspace1");
		CqlResult result1 = server.execute_cql3_query(ByteBufferUtil.bytes("SELECT * FROM test"), Compression.NONE, ConsistencyLevel.QUORUM);
        server.set_keyspace("Keyspace2");
        CqlResult result2 = server.execute_cql3_query(ByteBufferUtil.bytes("SELECT * FROM test"), Compression.NONE, ConsistencyLevel.QUORUM);

		assert result1.getRows().size() == 0 : "the result should be empty";
		assert result2.getRows().size() == 0 : "the result should be empty";

        server.set_keyspace("Keyspace1");
        server.execute_cql3_query(ByteBufferUtil.bytes("INSERT INTO test (id, num) VALUES ('someKey1', 123)"), Compression.NONE, ConsistencyLevel.QUORUM);
        server.set_keyspace("Keyspace2");
        server.execute_cql3_query(ByteBufferUtil.bytes("INSERT INTO test (id, num) VALUES ('someKey2', 123)"), Compression.NONE, ConsistencyLevel.QUORUM);
        server.execute_cql3_query(ByteBufferUtil.bytes("INSERT INTO test (id, num) VALUES ('someKey3', 123)"), Compression.NONE, ConsistencyLevel.QUORUM);

        server.set_keyspace("Keyspace1");
		result1 = server.execute_cql3_query(ByteBufferUtil.bytes("SELECT * FROM test"), Compression.NONE, ConsistencyLevel.QUORUM);
        server.set_keyspace("Keyspace2");
		result2 = server.execute_cql3_query(ByteBufferUtil.bytes("SELECT * FROM test"), Compression.NONE, ConsistencyLevel.QUORUM);

		assert result1.getRows().size() == 1 : "the result should contain one row";
		assert result2.getRows().size() == 2 : "the result should contain two row";
		
        server.set_keyspace("Keyspace1");
        server.execute_cql3_query(ByteBufferUtil.bytes("DROP TABLE test"), Compression.NONE, ConsistencyLevel.QUORUM);
        server.set_keyspace("Keyspace2");
        server.execute_cql3_query(ByteBufferUtil.bytes("DROP TABLE test"), Compression.NONE, ConsistencyLevel.QUORUM);
    }

    @Test
    public void select_parallel_prepared() throws Exception
    {
        CassandraServer server = new CassandraServer();
        
        server.set_keyspace("Keyspace1");
        server.execute_cql3_query(ByteBufferUtil.bytes("CREATE TABLE test (id text PRIMARY KEY, num int)"), Compression.NONE, ConsistencyLevel.QUORUM);
        server.set_keyspace("Keyspace2");
        server.execute_cql3_query(ByteBufferUtil.bytes("CREATE TABLE test (id text PRIMARY KEY, num int)"), Compression.NONE, ConsistencyLevel.QUORUM);

        server.set_keyspace("Keyspace1");
        CqlPreparedResult selectStatement1 = server.prepare_cql3_query(ByteBufferUtil.bytes("SELECT * FROM test"), Compression.NONE);
        server.set_keyspace("Keyspace2");
        CqlPreparedResult selectStatement2 = server.prepare_cql3_query(ByteBufferUtil.bytes("SELECT * FROM test"), Compression.NONE);
        
        List<ByteBuffer> bindVariables = new LinkedList<ByteBuffer>();

        server.set_keyspace("Keyspace1");
		CqlResult result1 = server.execute_prepared_cql3_query(selectStatement1.itemId, bindVariables , ConsistencyLevel.QUORUM);
        server.set_keyspace("Keyspace2");
		CqlResult result2 = server.execute_prepared_cql3_query(selectStatement2.itemId, bindVariables , ConsistencyLevel.QUORUM);

		assert result1.getRows().size() == 0 : "the result should be empty";
		assert result2.getRows().size() == 0 : "the result should be empty";

        server.set_keyspace("Keyspace1");
        server.execute_cql3_query(ByteBufferUtil.bytes("INSERT INTO test (id, num) VALUES ('someKey1', 123)"), Compression.NONE, ConsistencyLevel.QUORUM);
        server.set_keyspace("Keyspace2");
        server.execute_cql3_query(ByteBufferUtil.bytes("INSERT INTO test (id, num) VALUES ('someKey2', 123)"), Compression.NONE, ConsistencyLevel.QUORUM);
        server.execute_cql3_query(ByteBufferUtil.bytes("INSERT INTO test (id, num) VALUES ('someKey3', 123)"), Compression.NONE, ConsistencyLevel.QUORUM);

        server.set_keyspace("Keyspace1");
		result1 = server.execute_prepared_cql3_query(selectStatement1.itemId, bindVariables , ConsistencyLevel.QUORUM);
        server.set_keyspace("Keyspace2");
		result2 = server.execute_prepared_cql3_query(selectStatement2.itemId, bindVariables , ConsistencyLevel.QUORUM);

		assert result1.getRows().size() == 1 : "the result should contain one row, instead there where " + result1.getRows().size();
		assert result2.getRows().size() == 2 : "the result should contain two row, instead there where " + result2.getRows().size();
		
        server.set_keyspace("Keyspace1");
        server.execute_cql3_query(ByteBufferUtil.bytes("DROP TABLE test"), Compression.NONE, ConsistencyLevel.QUORUM);
        server.set_keyspace("Keyspace2");
        server.execute_cql3_query(ByteBufferUtil.bytes("DROP TABLE test"), Compression.NONE, ConsistencyLevel.QUORUM);
    }
    
}
