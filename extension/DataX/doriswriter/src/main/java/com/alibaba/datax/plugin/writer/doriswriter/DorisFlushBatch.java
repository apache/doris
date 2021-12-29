/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
  -->
 */
package com.alibaba.datax.plugin.writer.doriswriter;

import com.google.common.base.Strings;
import javax.xml.bind.DatatypeConverter;

// A wrapper class to hold a batch of loaded rows
public class DorisFlushBatch {
	private String lineDelimiter;
	private String label;
	private long rows = 0;
	private StringBuilder data = new StringBuilder();

	public DorisFlushBatch(String lineDelimiter) {
		if (lineDelimiter.startsWith("\\x")) {
			this.lineDelimiter = this.hexStringToString(lineDelimiter.replace("\\x",""));
		}else {
			this.lineDelimiter = lineDelimiter;
		}
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getLabel() {
		return label;
	}

	public long getRows() {
		return rows;
	}
	public  String hexStringToString(String s) {
		if (s == null || s.equals("")) {
			return null;
		}
		s = s.replace(" ", "");
		byte[] baKeyword = new byte[s.length() / 2];
		for (int i = 0; i < baKeyword.length; i++) {
			try {
				baKeyword[i] = (byte) (0xff & Integer.parseInt(
						s.substring(i * 2, i * 2 + 2), 16));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		try {
			s = new String(baKeyword, "gbk");
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		return s;
	}

	public int lineDelimiterLength(String hexString) {
		if (!Strings.isNullOrEmpty(hexString)) {
			if (!hexString.startsWith("\\x")) {
				return hexString.length();
			}
			hexString = hexString.replace("\\x","");
			return DatatypeConverter.parseHexBinary(hexString).length;
		}
		return 0;
	}

	public void putData(String row) {
		if (lineDelimiterLength(this.lineDelimiter) > 1) {
			data.append(row).append(lineDelimiter);
		} else {
			if (data.length() > 0) {
				data.append(lineDelimiter);
			}
			data.append(row);
		}
		rows++;
	}

	public StringBuilder getData() {
		return data;
	}

	public long getSize() {
		return data.length();
	}
}
