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

package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math/bits"

	_ "github.com/go-sql-driver/mysql"

	"github.com/RoaringBitmap/roaring/roaring64"
)

type BitmapReader interface {
	Query(sql string, args ...any) (*roaring64.Bitmap, error)
}

type bitmapReaderImpl struct {
	db *sql.DB
}

func NewBitmapReader(host string, port int, user, password string) BitmapReader {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port)

	//connect to doris
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}

	return &bitmapReaderImpl{
		db: db,
	}
}

func deserialize(bt []byte) (*roaring64.Bitmap, error) {
	bitmap := roaring64.NewBitmap()
	if len(bt) == 0 {
		return bitmap, nil
	}
	t := bt[0]
	bt = bt[1:]
	switch t {
	case 0:
		return bitmap, nil
	case 1:
		bitmap.Add(uint64(readUint32(bt)))
		return bitmap, nil
	case 2:
		_, e := bitmap.ReadFrom(bytes.NewReader(bt))
		return bitmap, e
	case 3:
		bitmap.Add(bits.ReverseBytes64(binary.BigEndian.Uint64(bt)))
		return bitmap, nil
	case 4:
		nbHighs, p := decodeVarint64(bt)
		bt = bt[p:] // 截断高位container length
		bytebuf := bytes.NewBuffer([]byte{})
		err := binary.Write(bytebuf, binary.BigEndian, bits.ReverseBytes64(uint64(nbHighs)))
		if err != nil {
			return bitmap, err
		}
		res := [][]byte{bytebuf.Bytes(), bt}
		join := bytes.Join(res, []byte("")) // 把[]byte 转换为roaring64
		_, err = bitmap.ReadFrom(bytes.NewReader(join))
		return bitmap, err
	}
	return bitmap, fmt.Errorf("未知的类型： %d", t)
}

func readUint32(bt []byte) uint32 {
	return bits.ReverseBytes32(binary.BigEndian.Uint32(bt))
}

func decodeVarint64(bt []byte) (int64, int) { // nolint
	result := int64(0)
	shift := int32(0)
	B := int16(128)
	idx := 0
	for {
		readByte := bt[idx]
		idx++
		oneByte := int16(readByte)
		isEnd := (oneByte & B) == 0
		result |= int64(oneByte&(B-1)) << (shift * 7)
		if isEnd {
			break
		}
		shift++
	}
	return result, idx
}

func (dao *bitmapReaderImpl) Query(sql string, args ...any) (*roaring64.Bitmap, error) {
	sql = "set return_object_data_as_binary=true;set enable_vectorized_engine=false;set enable_sql_cache = false;" + sql
	rows, err := dao.db.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	if rows.Next() {
		bt := make([]byte, 0)
		sErr := rows.Scan(&bt)
		if sErr != nil {
			return nil, sErr
		}
		return deserialize(bt)
	}
	return nil, err
}

func main() {
	reader := NewBitmapReader("127.0.0.1", 9030, "root", "")
	memberBitmap, err := reader.Query("select bitmap_union(members) from crowd where id=?", 1)
	if err != nil {
		panic(err)
	}

	fmt.Println(memberBitmap.GetCardinality())
	fmt.Println(memberBitmap.Contains(23223))
}
