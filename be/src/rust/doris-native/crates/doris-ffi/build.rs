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

//! Build script: compiles glibc compatibility stubs for older Linux systems.
//!
//! On glibc < 2.38, aws-lc-rs references symbols like __isoc23_sscanf that
//! don't exist. This build script detects the glibc version and compiles
//! stub implementations when needed.
//!
//! On newer glibc (>= 2.38), the stubs are skipped to avoid duplicate symbols.

fn main() {
    println!("cargo:rerun-if-changed=glibc_compat.c");

    // Only needed on Linux
    if std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default() != "linux" {
        return;
    }

    // Detect glibc version by parsing the output of `ldd --version`
    let needs_compat = match detect_glibc_version() {
        Some((major, minor)) => {
            println!(
                "cargo:warning=Detected glibc {}.{} — {}",
                major,
                minor,
                if major < 2 || (major == 2 && minor < 38) {
                    "compiling compat stubs"
                } else {
                    "skipping compat stubs (not needed)"
                }
            );
            major < 2 || (major == 2 && minor < 38)
        }
        None => {
            // Can't detect — compile stubs to be safe (use weak symbols to avoid conflicts)
            println!("cargo:warning=Cannot detect glibc version — compiling compat stubs with weak symbols");
            true
        }
    };

    if needs_compat {
        cc::Build::new()
            .file("glibc_compat.c")
            .warnings(false)
            .compile("glibc_compat");
    }
}

fn detect_glibc_version() -> Option<(u32, u32)> {
    // Try ldd --version (works on most Linux)
    let output = std::process::Command::new("ldd")
        .arg("--version")
        .output()
        .ok()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let text = if stdout.contains("GLIBC") || stdout.contains("glibc") || stdout.contains("ldd") {
        stdout
    } else {
        stderr
    };

    // Parse "ldd (GNU libc) 2.17" or "ldd (Ubuntu GLIBC 2.35-0ubuntu3) 2.35"
    for line in text.lines() {
        // Find version number pattern: major.minor
        let parts: Vec<&str> = line.split_whitespace().collect();
        for part in parts.iter().rev() {
            if let Some((major_str, minor_str)) = part.split_once('.') {
                if let (Ok(major), Ok(minor)) = (major_str.parse::<u32>(), minor_str.parse::<u32>())
                {
                    if major >= 2 && minor < 100 {
                        return Some((major, minor));
                    }
                }
            }
        }
    }
    None
}
