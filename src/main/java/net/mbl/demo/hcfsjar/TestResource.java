package net.mbl.demo.hcfsjar;

import org.apache.hadoop.fs.Path;

import java.net.URL;

public class TestResource {
    public static Path getResourcePath(String resPath) {
        URL url = TestResource.class.getResource(resPath);
        String purePath = url.getPath();
        if (purePath != null && purePath.startsWith("file:")) {
            purePath = purePath.substring(5);
        }
        Path path = new Path(url.getProtocol(), url.getAuthority(), purePath);
        return path;
    }
}
