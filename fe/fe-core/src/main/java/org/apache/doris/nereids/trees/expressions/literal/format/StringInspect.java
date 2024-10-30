package org.apache.doris.nereids.trees.expressions.literal.format;

public class StringInspect {
    public final String str;
    private int index;

    public StringInspect(String str) {
        this.str = str;
    }

    public boolean eos() {
        return index >= str.length();
    }

    public int remain() {
        return str.length() - index;
    }
    
    public char lookAt() {
        return str.charAt(index);
    }

    public void step() {
        this.index++;
    }

    public void step(int step) {
        this.index += step;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public char lookAndStep() {
        return str.charAt(index++);
    }

    public int index() {
        return index;
    }
}
