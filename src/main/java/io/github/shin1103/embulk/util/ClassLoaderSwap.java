package io.github.shin1103.embulk.util;

/*
    This Class is based on embulk-output-parquet ClassLoaderSwap.java
    https://github.com/choplin/embulk-output-parquet/blob/master/src/main/java/org/embulk/output/parquet/ClassLoaderSwap.java
 */
public class ClassLoaderSwap<T> implements AutoCloseable
{
    private final ClassLoader orgClassLoader;
    private final Thread curThread;

    public ClassLoaderSwap(Class<T> pluginClass)
    {
        this.curThread = Thread.currentThread();
        ClassLoader pluginClassLoader = pluginClass.getClassLoader();
        this.orgClassLoader = curThread.getContextClassLoader();
        curThread.setContextClassLoader(pluginClassLoader);
    }

    @Override
    public void close()
    {
        curThread.setContextClassLoader(orgClassLoader);
    }
}
