package org.apache.doris.persist.gson;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils.HiddenAnnotationExclusionStrategy;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

/*
 * This unit test shows how to serialize and deserialize inherited class.
 * 
 * ParentClass is the parent class of 2 derived classes:
 *      ChildClassA
 *      ChildClassB
 *      
 * User need to create a RuntimeTypeAdapterFactory for ParentClass and
 * register 2 derived classes to the factory. And then register the factory
 * to the GsonBuilder to create GSON instance.
 * 
 * Notice that there is a special field "clazz" in ParentClass. This field is used
 * to help the RuntimeTypeAdapterFactory to distinguish the kind of derived class.
 * This field's name should be specified when creating the RuntimeTypeAdapterFactory.
 * 
 */
public class GsonDerivedClassSerializationTest {
    private static String fileName = "./GsonDerivedClassSerializationTest";
    
    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    public static class ParentClass implements Writable {
        @SerializedName(value = "flag")
        public int flag = 0;
        @SerializedName(value = "clazz")
        public String clazz; // a specified field to save the type of derived classed

        public ParentClass(int flag, String clazz) {
            this.flag = flag;
            this.clazz = clazz;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            String json = TEST_GSON.toJson(this);
            System.out.println("write: " + json);
            Text.writeString(out, json);
        }

        public static ParentClass read(DataInput in) throws IOException {
            String json = Text.readString(in);
            System.out.println("read: " + json);
            return TEST_GSON.fromJson(json, ParentClass.class);
        }
    }

    public static class ChildClassA extends ParentClass {
        @SerializedName(value = "tag")
        public String tagA;

        public ChildClassA(int flag, String tag) {
            // pass "ChildClassA.class.getSimpleName()" to field "clazz"
            super(flag, ChildClassA.class.getSimpleName());
            this.tagA = tag;
        }
    }

    public static class ChildClassB extends ParentClass {
        @SerializedName(value = "mapB")
        public Map<Long, String> mapB = Maps.newConcurrentMap();

        public ChildClassB(int flag) {
            // pass "ChildClassB.class.getSimpleName()" to field "clazz"
            super(flag, ChildClassB.class.getSimpleName());
            this.mapB.put(1L, "B1");
            this.mapB.put(2L, "B2");
        }
    }

    private static RuntimeTypeAdapterFactory<ParentClass> runtimeTypeAdapterFactory = RuntimeTypeAdapterFactory
            // the "clazz" here is the name of "clazz" field in ParentClass.
            .of(ParentClass.class, "clazz")
            // register 2 derived classes, the second parameter must be same to the value of field "clazz"
            .registerSubtype(ChildClassA.class, ChildClassA.class.getSimpleName())
            .registerSubtype(ChildClassB.class, ChildClassB.class.getSimpleName());

    private static Gson TEST_GSON = new GsonBuilder()
            .addSerializationExclusionStrategy(new HiddenAnnotationExclusionStrategy())
            .enableComplexMapKeySerialization()
            // register the RuntimeTypeAdapterFactory
            .registerTypeAdapterFactory(runtimeTypeAdapterFactory)
            .create();

    @Test
    public void testDerivedClassA() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        ChildClassA childClassA = new ChildClassA(1, "A");
        childClassA.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        ParentClass parentClass = ParentClass.read(in);
        Assert.assertTrue(parentClass instanceof ChildClassA);
        Assert.assertEquals(1, ((ChildClassA) parentClass).flag);
        Assert.assertEquals("A", ((ChildClassA) parentClass).tagA);
    }

    @Test
    public void testDerivedClassB() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        ChildClassB childClassB = new ChildClassB(2);
        childClassB.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        ParentClass parentClass = ParentClass.read(in);
        Assert.assertTrue(parentClass instanceof ChildClassB);
        Assert.assertEquals(2, ((ChildClassB) parentClass).flag);
        Assert.assertEquals(2, ((ChildClassB) parentClass).mapB.size());
        Assert.assertEquals("B1", ((ChildClassB) parentClass).mapB.get(1L));
        Assert.assertEquals("B2", ((ChildClassB) parentClass).mapB.get(2L));
    }

}
