package org.marianoguerra.astor;

import java.io.IOException;

public interface EventHolder {
    long size() throws IOException;
    long count() throws IOException;
    long oldest() throws IOException;
    long newest() throws IOException;
}
