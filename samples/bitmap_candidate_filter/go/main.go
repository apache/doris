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
	"context"
	"database/sql"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/go-sql-driver/mysql"
)

const (
	fixtureRows = uint64(20_000_000)
	baseID      = uint64(1) << 32
	needleEvery = uint64(97)
)

type result struct {
	count int64
	sum   int64
}

type candidateSet struct {
	encoded       string
	portableBytes int
	buildTime     time.Duration
	encodeTime    time.Duration
	expected      [2]result
}

func makeCandidateSet(size uint64) (candidateSet, error) {
	if size == 0 || fixtureRows%size != 0 {
		return candidateSet{}, fmt.Errorf("size %d must divide fixture row count %d", size, fixtureRows)
	}
	start := time.Now()
	bitmap := roaring64.NewBitmap()
	step := fixtureRows / size
	var expected [2]result
	for offset := uint64(0); offset < fixtureRows; offset += step {
		id := baseID + offset
		bitmap.Add(id)
		shape := 1
		if offset%needleEvery == 0 {
			shape = 0
		}
		expected[shape].count++
		expected[shape].sum += int64(id)
	}
	bitmap.RunOptimize()
	buildTime := time.Since(start)
	if bitmap.GetCardinality() != size {
		return candidateSet{}, fmt.Errorf("bitmap cardinality %d, expected %d", bitmap.GetCardinality(), size)
	}

	start = time.Now()
	var portable bytes.Buffer
	if _, err := bitmap.WriteTo(&portable); err != nil {
		return candidateSet{}, fmt.Errorf("serialize portable bitmap: %w", err)
	}
	encoded := base64.StdEncoding.EncodeToString(portable.Bytes())
	return candidateSet{
		encoded:       encoded,
		portableBytes: portable.Len(),
		buildTime:     buildTime,
		encodeTime:    time.Since(start),
		expected:      expected,
	}, nil
}

func parseSizes(value string) ([]uint64, error) {
	var sizes []uint64
	for _, field := range strings.Split(value, ",") {
		size, err := strconv.ParseUint(strings.TrimSpace(field), 10, 64)
		if err != nil || size == 0 || fixtureRows%size != 0 {
			return nil, fmt.Errorf("invalid size %q: use positive divisors of %d", field, fixtureRows)
		}
		sizes = append(sizes, size)
	}
	return sizes, nil
}

func measure(conn *sql.Conn, query, encoded, pattern string, expected result, runs int, timeout time.Duration) (time.Duration, time.Duration, time.Duration, error) {
	var total, minimum, maximum time.Duration
	for i := 0; i <= runs; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()
		var actual result
		err := conn.QueryRowContext(ctx, query, int64(101), encoded, pattern).Scan(&actual.count, &actual.sum)
		elapsed := time.Since(start)
		cancel()
		if err != nil {
			return 0, 0, 0, err
		}
		if actual != expected {
			return 0, 0, 0, fmt.Errorf("%s: got count=%d sum=%d, expected count=%d sum=%d",
				pattern, actual.count, actual.sum, expected.count, expected.sum)
		}
		if i == 0 { // Warm-up query is validated but excluded from timing.
			continue
		}
		if i == 1 || elapsed < minimum {
			minimum = elapsed
		}
		if elapsed > maximum {
			maximum = elapsed
		}
		total += elapsed
	}
	return total / time.Duration(runs), minimum, maximum, nil
}

func run() error {
	dsn := flag.String("dsn", os.Getenv("DORIS_DSN"), "Doris MySQL DSN (or DORIS_DSN); defaults to local root")
	table := flag.String("table", "", "fully qualified benchmark fact table")
	sizeList := flag.String("sizes", "1000000", "comma-separated candidate counts")
	runs := flag.Int("runs", 5, "timed queries per text shape, after one warm-up")
	timeout := flag.Duration("timeout", 2*time.Minute, "timeout per query")
	iceberg := flag.Bool("iceberg", false, "enable the Doris file scanner v2 for Iceberg")
	flag.Parse()
	if *table == "" || !regexp.MustCompile(`^[A-Za-z_][A-Za-z_0-9]*(\.[A-Za-z_][A-Za-z_0-9]*){0,2}$`).MatchString(*table) {
		return fmt.Errorf("-table must be a one-, two-, or three-part SQL identifier")
	}
	if *runs < 1 {
		return fmt.Errorf("-runs must be positive")
	}
	sizes, err := parseSizes(*sizeList)
	if err != nil {
		return err
	}
	if *dsn == "" {
		*dsn = "root@tcp(127.0.0.1:9030)/"
	}
	cfg, err := mysql.ParseDSN(*dsn)
	if err != nil {
		return fmt.Errorf("parse DSN: %w", err)
	}
	// This sends the large bitmap in one SQL request, including its actual wire cost.
	cfg.InterpolateParams = true
	cfg.MaxAllowedPacket = 0
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("connect to Doris: %w", err)
	}
	defer conn.Close()
	for _, setting := range []string{"SET enable_sql_cache = false", "SET enable_query_cache = false"} {
		if _, err := conn.ExecContext(ctx, setting); err != nil {
			return fmt.Errorf("%s: %w", setting, err)
		}
	}
	if *iceberg {
		if _, err := conn.ExecContext(ctx, "SET enable_file_scanner_v2 = true"); err != nil {
			return err
		}
	}
	query := fmt.Sprintf(`SELECT COUNT(*), COALESCE(SUM(resource_id), 0) FROM %s
WHERE org_id = ? AND bitmap_contains(bitmap_from_roaring_base64(?), resource_id)
AND payload LIKE ?`, *table)
	fmt.Println("table,size,shape,portable_bytes,base64_bytes,build_ms,encode_ms,query_mean_ms,query_min_ms,query_max_ms,end_to_end_mean_ms")
	for _, size := range sizes {
		candidates, err := makeCandidateSet(size)
		if err != nil {
			return err
		}
		for shape, pattern := range []string{"%needle%", "%other%"} {
			mean, min, max, err := measure(conn, query, candidates.encoded, pattern, candidates.expected[shape], *runs, *timeout)
			if err != nil {
				return fmt.Errorf("size %d: %w", size, err)
			}
			fmt.Printf("%s,%d,%s,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n",
				*table, size, strings.Trim(pattern, "%"), candidates.portableBytes, len(candidates.encoded),
				ms(candidates.buildTime), ms(candidates.encodeTime), ms(mean), ms(min), ms(max),
				ms(candidates.buildTime+candidates.encodeTime+mean))
		}
	}
	return nil
}

func ms(duration time.Duration) float64 { return float64(duration) / float64(time.Millisecond) }

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
