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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"math"
	"os"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/go-sql-driver/mysql"
)

func portableRoaring32(t *testing.T) string {
	t.Helper()
	bitmap := roaring.NewBitmap()
	bitmap.Add(1)
	bitmap.Add(math.MaxUint32)
	var portable bytes.Buffer
	if _, err := bitmap.WriteTo(&portable); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(portable.Bytes())
}

func TestRoaring32PortableBase64(t *testing.T) {
	encoded := portableRoaring32(t)
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	decoded := roaring.NewBitmap()
	if _, err := decoded.ReadFrom(bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if decoded.GetCardinality() != 2 || !decoded.Contains(1) || !decoded.Contains(math.MaxUint32) {
		t.Fatalf("unexpected Roaring32 values: %v", decoded.ToArray())
	}
}

func TestRoaring32Doris(t *testing.T) {
	dsn := os.Getenv("DORIS_DSN")
	if dsn == "" {
		t.Skip("set DORIS_DSN to test the Roaring32 client against Doris")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.InterpolateParams = true
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	encoded := portableRoaring32(t)
	for _, test := range []struct {
		id   int64
		want int
	}{
		{1, 1},
		{math.MaxUint32, 1},
		{2, 0},
		{int64(math.MaxUint32) + 1, 0},
	} {
		var got int
		err := db.QueryRowContext(ctx,
			"SELECT bitmap_contains(bitmap_from_roaring_base64(?), ?)", encoded, test.id).Scan(&got)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Errorf("resource_id %d: got %d, want %d", test.id, got, test.want)
		}
	}
}
