package org.apache.doris.nereids.trees.expressions.literal.format;

public class StringChecker extends FormatChecker {
    private String str;

    public StringChecker(StringInspect stringInspect, String str) {
        super(stringInspect);
        this.str = str;
    }

    @Override
    protected boolean doCheck() {
        if (stringInspect.remain() < str.length()) {
            return false;
        }

        for (int i = 0; i < str.length(); i++) {
            if (stringInspect.lookAndStep() != str.charAt(i)) {
                return false;
            }
        }
        return true;
    }
}
