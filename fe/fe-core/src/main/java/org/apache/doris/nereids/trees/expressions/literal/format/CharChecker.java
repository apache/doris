package org.apache.doris.nereids.trees.expressions.literal.format;

public class CharChecker extends FormatChecker {
    public final char c;

    public CharChecker(StringInspect stringInspect, char c) {
        super(stringInspect);
        this.c = c;
    }

    @Override
    protected boolean doCheck() {
        if (stringInspect.eos() || stringInspect.lookAndStep() != c) {
            return false;
        }
        return true;
    }
}
