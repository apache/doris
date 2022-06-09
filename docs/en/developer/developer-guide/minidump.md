---
{
    "title": "Minidump",
    "language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Minidump(removed)

> Minidump has been removed, it's useless in real online environment and instead introduces additional bugs

Minidump is a file format defined by Microsoft for reporting errors after program crashes. It includes thread information, register information, call stack information, etc. at the time of the crash, which helps developers quickly locate the problem.

Unlike [Coredump](https://en.wikipedia.org/wiki/Core_dump), Minidump files are smaller and easier to report and network transmission. Coredump file will contain a complete memory image, so the volume may be dozens or hundreds of GB. The Minidump file only contains the call stack and register information of the key thread, so the size is usually only MB level.

[Breakpad](https://github.com/google/breakpad) is a cross-platform crash dump and analysis framework and tool collection. Users can use Breakpad to conduct self-service analysis of Minidump files. You can also collect Minidump files and report them to Doris cluster operation and maintenance or developers.

## How to enable Minidump

Minidump function is a function introduced in Doris 0.15.0 or later. This function is controlled by the following configuration files of BE:

* `disable_minidump`

    Whether to enable Minidump function. The default is false, which means it is turned on.
    
* `minidump_dir`

    The storage directory of the Minidump file. The default is `${DORIS_HOME}/Minidump/`
    
* `max_minidump_file_size_mb`

    Minidump file size limit. The default is 200MB. If the size exceeds the threshold, breakpad will try to reduce the information recorded in the file, such as the number of threads and the number of registers to introduce the Minidump file size. But this is only an expected value, and the actual file size may be larger than the set value.
    
* `max_minidump_file_number`

    The maximum number of Minidump files to keep. The default is 10, which means that the most recent 10 files are kept.
    
## How to generate Minidump

There are two ways to generate Minidump:

1. The program crashes

    When the program encounters a problem and crashes, it will automatically generate a Minidump file. The following information will appear in be.out at this time:
    
    ```
    Minidump created at: /doris/be/Minidump/4f8d4fe5-15f8-40a3-843109b3-d49993f3.dmp
    *** Aborted at 1636970042 (unix time) try "date -d @1636970042" if you are using GNU date ***
    PC: @ 0x1b184e4 doris::OlapScanNode::scanner_thread()
    *** SIGSEGV (@0x0) received by PID 71567 (TID 0x7f173a5df700) from PID 0; stack trace: ***
    @ 0x220c992 google::(anonymous namespace)::FailureSignalHandler()
    @ 0x7f174fb5e1d0 (unknown)
    @ 0x1b184e4 doris::OlapScanNode::scanner_thread()
    @ 0x15a19af doris::PriorityThreadPool::work_thread()
    @ 0x21d9107 thread_proxy
    @ 0x7f174fb53f84 start_thread
    @ 0x7f174f943ddf __GI___clone
    @ 0x0 (unknown)
    ```
    
    Among them, `/doris/be/Minidump/4f8d4fe5-15f8-40a3-843109b3-d49993f3.dmp` is the Minidump file. And the following stack is the call stack information where the program crashed.
    
2. Manual trigger

    The user can actively send the SIGUSR1 signal to the BE process to trigger Minidump. For example, use the following command:
    
    ```
    kill -s SIGUSR1 71567
    ```
    
    71567 is the process id (pid) of BE. After that, the following information will appear in be.out:
    
    ```
    Receive signal: SIGUSR1
    Minidump created at: /doris/be/Minidump/1af8fe8f-3d5b-40ea-6b76ad8f-0cf6756f.dmp
    ```

    Among them, `Receive signal: SIGUSR1` means that this is a Minidump operation triggered by the user. Following is the location of the Minidump file.
    
    The Minidump operation manually triggered by the user will not kill the BE process and will not generate an error stack in be.out.
    
## How to analyze Minidump

We can use various tools provided by breakpad to analyze Minidump to see the cause of the error.

### Get the breakpad tool

Users can go to [Breakpad](https://github.com/google/breakpad) code base to download and compile breakpad. For the compilation method, please refer to the `build_breakpad()` method in [thirdparty/vars.sh](https://github.com/apache/incubator-doris/blob/master/thirdparty/vars.sh) in the Doris source code library.

You can also find various tools compiled by breakpad from the `/var/local/thirdparty/installed/bin` directory of the image container in the version 1.4.2 and above of the Docker compiled image provided by Doris.

### Analyze Minidump

We can use the following two methods to analyze Minidump files.

1. Dump into coredump file

    Use the `minidump-2-core` tool provided by breakpad to dump the Minidump file into a coredump file:
    
    ```
    ./minidump-2-core /doris/be/Minidump/1af8fe8f-3d5b-40ea-6b76ad8f-0cf6756f.dmp> 1.coredump
    ```
    
    Then we can use the gdb tool to analyze the coredump file:
    
    ```
    gdb lib/palo_be -c 1.coredump
    ```

2. Generate a readable call stack

    The Minidump file only contains the address of the call stack, and we need to map these addresses to the actual function file location. Therefore, we first need to generate the symbol table `palo_be.sym` of the BE binary file through `dump_syms`:
    
    ```
    ./dump_syms ./lib/palo_be> palo_be.sym
    ```

    Next, we need the information in the first row of the symbol table to build a corresponding symbol table directory.
    
    ```
    head -n1 palo_be.sym
    ```
    
    The above command will print the first line of palo_be.sym as follows:
    
    ```
    MODULE Linux x86_64 137706CC745F5EC3EABBF730D4B229370 palo_be
    ```
    
    Then we create a directory structure:
    
    ```
    mkdir -p ./symbols/palo_be/137706CC745F5EC3EABBF730D4B229370
    ```
    
    The `palo_be` and `137706CC745F5EC3EABBF730D4B229370` in the directory path must be consistent with the first line of the palo_be.sym file. Then we move the palo_be.sym file to this directory:
    
    ```
    cp palo_be.sym ./symbols/palo_be/137706CC745F5EC3EABBF730D4B229370
    ```
    
    Finally, we can use `minidump_stackwalk` to produce readable call stack information:
    
    ```
    minidump_stackwalk 4f8d4fe5-15f8-40a3-843109b3-d49993f3.dmp ./symbols/> readable.stack
    ```
    
    Among them, `4f8d4fe5-15f8-40a3-843109b3-d49993f3.dmp` is a minidump file. `./symbols/` is the previously created directory containing palo_be.sym. `readable.stack` redirects the generated results to this file. At the same time, when this command is executed, some program running logs will be flashed on the screen, so you can ignore it.
    
    At this point, we have obtained a readable thread call stack file: readable.stack. It contains the call stack information of all threads when the BE program is writing the Minidump file, and the corresponding register information. Among them, `Crash reason` explains why the program crashed. If it is `DUMP_REQUESTED`, it means that this is a Minidump triggered by the user.
    
     We can filter out the register information with the following command to get a clear view of the call stack:
    
     ```
     grep -v = readable.stack |grep -v "Found by" |vi-
     ```
    
     The result is similar to the thread call stack information obtained through the pstack command.