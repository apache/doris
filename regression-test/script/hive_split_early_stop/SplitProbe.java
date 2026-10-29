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

import com.sun.tools.attach.VirtualMachine;
import jdk.internal.org.objectweb.asm.ClassReader;
import jdk.internal.org.objectweb.asm.ClassVisitor;
import jdk.internal.org.objectweb.asm.ClassWriter;
import jdk.internal.org.objectweb.asm.MethodVisitor;
import jdk.internal.org.objectweb.asm.Opcodes;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/** Test-only observer. Strong references let the test verify explicit disposal independently of GC. */
public class SplitProbe {
    private static final Map<Object, Record> RECORDS = new IdentityHashMap<>();
    private static PrintWriter output;

    private static class Record {
        final Object assignment;
        final int id;
        final String query;
        final AtomicInteger running = new AtomicInteger();

        Record(Object assignment, int id, String query) {
            this.assignment = assignment;
            this.id = id;
            this.query = query;
        }
    }

    private static Object field(Object object, String name) throws ReflectiveOperationException {
        Field field = object.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(object);
    }

    public static synchronized void track(Object assignment) {
        try {
            Class<?> contextClass = Class.forName("org.apache.doris.qe.ConnectContext", false,
                    assignment.getClass().getClassLoader());
            Object context = contextClass.getMethod("get").invoke(null);
            Object query = contextClass.getMethod("queryId").invoke(context);
            String id = String.format("%016x-%016x", query.getClass().getMethod("getHi").invoke(query),
                    query.getClass().getMethod("getLo").invoke(query));
            RECORDS.put(assignment, new Record(assignment, RECORDS.size() + 1, id));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot observe Split assignment", e);
        }
    }

    public static synchronized void enter(Object producer) {
        change(producer, 1);
    }

    public static synchronized void leave(Object producer) {
        change(producer, -1);
    }

    private static void change(Object producer, int delta) {
        try {
            Record record = RECORDS.get(field(producer, "assignment"));
            record.running.addAndGet(delta);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot observe Split producer", e);
        }
    }

    public static void premain(String file, Instrumentation instrumentation) throws Exception {
        output = new PrintWriter(new FileOutputStream(file), true);
        output.println("time_ms,assignment_id,query_id,stopped,finished,nonempty_queues,queued_ranges,"
                + "listeners,resources,active_runs,live_producers,terminated");
        instrumentation.addTransformer(new ClassFileTransformer() {
            @Override
            public byte[] transform(ClassLoader loader, String name, Class<?> type,
                    ProtectionDomain domain, byte[] bytes) {
                boolean assignment = name.equals("org/apache/doris/datasource/split/SplitAssignment");
                boolean producer = name.equals("org/apache/doris/datasource/split/SplitProducer");
                if (!assignment && !producer) {
                    return null;
                }
                ClassWriter writer = new ClassWriter(0);
                new ClassReader(bytes).accept(new ClassVisitor(Opcodes.ASM8, writer) {
                    @Override
                    public MethodVisitor visitMethod(int access, String method, String descriptor,
                            String signature, String[] exceptions) {
                        MethodVisitor visitor = super.visitMethod(access, method, descriptor, signature, exceptions);
                        boolean constructor = assignment && method.equals("<init>");
                        boolean run = producer && method.equals("run") && descriptor.equals("()V");
                        if (!constructor && !run) {
                            return visitor;
                        }
                        return new MethodVisitor(Opcodes.ASM8, visitor) {
                            private void call(String operation) {
                                super.visitVarInsn(Opcodes.ALOAD, 0);
                                super.visitMethodInsn(Opcodes.INVOKESTATIC, "SplitProbe", operation,
                                        "(Ljava/lang/Object;)V", false);
                            }

                            @Override
                            public void visitCode() {
                                super.visitCode();
                                if (run) {
                                    call("enter");
                                }
                            }

                            @Override
                            public void visitInsn(int opcode) {
                                if (opcode == Opcodes.RETURN) {
                                    call(constructor ? "track" : "leave");
                                }
                                super.visitInsn(opcode);
                            }

                            @Override
                            public void visitMaxs(int stack, int locals) {
                                super.visitMaxs(Math.max(stack, 1), locals);
                            }
                        };
                    }
                }, 0);
                return writer.toByteArray();
            }
        });
        Thread sampler = new Thread(SplitProbe::sample, "split-test-observer");
        sampler.setDaemon(true);
        sampler.start();
    }

    private static void sample() {
        try {
            while (true) {
                List<Record> snapshot;
                synchronized (SplitProbe.class) {
                    snapshot = new ArrayList<>(RECORDS.values());
                }
                for (Record record : snapshot) {
                    String row;
                    synchronized (field(record.assignment, "readinessLock")) {
                        int nonemptyQueues = 0;
                        int ranges = 0;
                        Map<?, ?> queues = (Map<?, ?>) field(record.assignment, "assignment");
                        for (Object value : queues.values()) {
                            synchronized (value) {
                                Collection<?> queue = (Collection<?>) field(value, "splits");
                                int size = queue.size();
                                nonemptyQueues += size == 0 ? 0 : 1;
                                ranges += size;
                            }
                        }
                        int waiters = 0;
                        Map<?, ?> listeners = (Map<?, ?>) field(record.assignment, "queueSpaceListeners");
                        synchronized (listeners) {
                            for (Object value : listeners.values()) {
                                waiters += ((Set<?>) value).size();
                            }
                        }
                        int resources = ((List<?>) field(record.assignment, "closeableResources")).size();
                        boolean terminated = ((CompletableFuture<?>) field(record.assignment, "termination")).isDone();
                        row = String.format("%d,%d,%s,%s,%s,%d,%d,%d,%d,%d,%s,%s", System.currentTimeMillis(),
                                record.id, record.query, field(record.assignment, "isStopped"),
                                field(record.assignment, "scheduleFinished"), nonemptyQueues, ranges, waiters, resources,
                                record.running.get(), field(record.assignment, "liveProducers"), terminated);
                    }
                    output.println(row);
                }
                output.flush();
                Thread.sleep(100);
            }
        } catch (Exception e) {
            // Keep diagnostics out of the CSV. A stopped sampler makes the acceptance report fail.
            e.printStackTrace(System.err);
        }
    }

    public static void agentmain(String file, Instrumentation instrumentation) throws Exception {
        Object processor = Class.forName("org.apache.doris.qe.QeProcessorImpl").getField("INSTANCE").get(null);
        Map<?, ?> callbacks = (Map<?, ?>) field(field(processor, "queryFinishCallbackRegistry"), "callbacks");
        try (PrintWriter writer = new PrintWriter(file)) {
            writer.println("query_id,callbacks");
            for (Map.Entry<?, ?> entry : callbacks.entrySet()) {
                writer.println(entry.getKey() + "," + ((List<?>) entry.getValue()).size());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Expected: <FE pid> <absolute probe jar> <registry output>");
        }
        VirtualMachine vm = VirtualMachine.attach(args[0]);
        try {
            vm.loadAgent(args[1], args[2]);
        } finally {
            vm.detach();
        }
    }
}
