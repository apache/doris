package org.apache.doris.common;

import org.apache.doris.journal.bdbje.BDBToolOptions;

import org.junit.Assert;
import org.junit.Test;

public class CommandLineOptionsTest {

    @Test
    public void test() {
        CommandLineOptions options = new CommandLineOptions(true, "", null);
        Assert.assertTrue(options.isVersion());
        Assert.assertFalse(options.runBdbTools());

        options = new CommandLineOptions(false, "", new BDBToolOptions(true, "", false, "", "", 0));
        Assert.assertFalse(options.isVersion());
        Assert.assertTrue(options.runBdbTools());
    }

}
