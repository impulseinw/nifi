package org.apache.nifi.util.verifier;

import org.apache.nifi.util.MockFlowFile;

public class ContentEqual implements Condition {

    protected final String content;

    public ContentEqual(String content) {
        super();
        this.content = content;
    }

    @Override
    public boolean check(MockFlowFile flowFile) {
        try {
            flowFile.assertContentEquals(content);
        } catch (AssertionError cf) {
            return false;
        }
        return true;
    }

}
