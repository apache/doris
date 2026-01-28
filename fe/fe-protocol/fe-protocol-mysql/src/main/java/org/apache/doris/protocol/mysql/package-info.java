// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/**
 * MySQL Protocol Implementation Package.
 * 
 * <h2>Overview</h2>
 * <p>This package contains the MySQL wire protocol implementation for Apache Doris.
 * It provides all the necessary classes to handle MySQL client connections, including
 * packet serialization, capability negotiation, and command handling.
 * 
 * <h2>Package Contents</h2>
 * <ul>
 *   <li><b>Protocol Definitions</b>
 *     <ul>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlCapability} - Capability flags</li>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlCommand} - Protocol commands</li>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlServerStatusFlag} - Server status flags</li>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlColType} - Column type codes</li>
 *     </ul>
 *   </li>
 *   <li><b>Packet Classes</b>
 *     <ul>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlPacket} - Base packet class</li>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlHandshakePacket} - Server handshake</li>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlAuthPacket} - Client auth response</li>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlAuthSwitchPacket} - Auth switch request</li>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlOkPacket} - OK response</li>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlErrPacket} - Error response</li>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlEofPacket} - EOF marker</li>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlClearTextPacket} - Clear text password</li>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlSslPacket} - SSL request</li>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlColDef} - Column definition</li>
 *       <li>{@link org.apache.doris.protocol.mysql.FieldInfo} - Column metadata</li>
 *     </ul>
 *   </li>
 *   <li><b>Serialization &amp; Protocol Utilities</b>
 *     <ul>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlSerializer} - Protocol serializer</li>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlProto} - Protocol read utilities</li>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlPassword} - Password handling</li>
 *       <li>{@link org.apache.doris.protocol.mysql.BytesChannel} - Channel interface</li>
 *       <li>{@link org.apache.doris.protocol.mysql.SslEngineHelper} - SSL utilities</li>
 *     </ul>
 *   </li>
 *   <li><b>SPI Implementation</b>
 *     <ul>
 *       <li>{@link org.apache.doris.protocol.mysql.MysqlProtocolHandler} - Protocol handler</li>
 *     </ul>
 *   </li>
 * </ul>
 * 
 * <h2>Migration from fe-core</h2>
 * <p>Classes in this package are the canonical protocol definitions. The original
 * classes in {@code org.apache.doris.mysql} (fe-core) are kept for backward compatibility
 * and may extend or delegate to these classes.
 * 
 * <h3>Mapping Table</h3>
 * <table border="1">
 *   <tr><th>Protocol Module (New)</th><th>fe-core (Legacy)</th></tr>
 *   <tr><td>{@code o.a.d.protocol.mysql.MysqlCapability}</td><td>{@code o.a.d.mysql.MysqlCapability}</td></tr>
 *   <tr><td>{@code o.a.d.protocol.mysql.MysqlCommand}</td><td>{@code o.a.d.mysql.MysqlCommand}</td></tr>
 *   <tr><td>{@code o.a.d.protocol.mysql.MysqlPacket}</td><td>{@code o.a.d.mysql.MysqlPacket}</td></tr>
 *   <tr><td>{@code o.a.d.protocol.mysql.MysqlSerializer}</td><td>{@code o.a.d.mysql.MysqlSerializer}</td></tr>
 * </table>
 * 
 * <h2>Backward Compatibility</h2>
 * <p>This package guarantees:
 * <ul>
 *   <li>Packet format unchanged - all MySQL clients continue to work</li>
 *   <li>Encryption/decryption logic unchanged</li>
 *   <li>Handshake protocol unchanged</li>
 *   <li>All MySQL commands supported</li>
 * </ul>
 * 
 * <h2>Usage</h2>
 * <pre>{@code
 * // Creating a serializer
 * MysqlSerializer serializer = MysqlSerializer.newInstance();
 * 
 * // Writing protocol data
 * serializer.writeInt4(12345);
 * serializer.writeLenEncodedString("Hello");
 * 
 * // Getting the byte buffer
 * ByteBuffer buffer = serializer.toByteBuffer();
 * }</pre>
 * 
 * @since 2.0.0
 */
package org.apache.doris.protocol.mysql;
