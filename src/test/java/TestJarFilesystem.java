import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import net.mbl.demo.hcfsjar.JarFilesystem;
import net.mbl.demo.hcfsjar.TestResource;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class TestJarFilesystem {

  @Test
  public void testLs() throws IOException {
    Set<String> expectChildName = new HashSet<>();
    expectChildName.add("maven");
    expectChildName.add("MANIFEST.MF");

    Configuration conf = new Configuration();
    conf.set("fs.jar.impl", JarFilesystem.class.getName());
    Path path = TestResource.getResourcePath("/hcfsdemo-1.0-SNAPSHOT.jar");
    Path jarPath = new Path("jar", "", path.toUri().getPath() + "!/META-INF/");
    FileStatus[] fileStatusArray = jarPath.getFileSystem(conf).listStatus(jarPath);

    Assert.assertEquals(2, fileStatusArray.length);
    for (FileStatus status : fileStatusArray) {
      Assert.assertTrue(expectChildName.contains(status.getPath().getName()));
    }
  }
  
  @Test
  public void testRead() throws IOException {
    Set<String> expectChildName = new HashSet<>();
    expectChildName.add("maven");
    expectChildName.add("MANIFEST.MF");

    Configuration conf = new Configuration();
    conf.set("fs.jar.impl", JarFilesystem.class.getName());
    Path path = TestResource.getResourcePath("/hcfsdemo-1.0-SNAPSHOT.jar");
    Path jarPath = new Path("jar", "",
        path.toUri().getPath() + "!/net/mbl/demo/hcfsdemo/Main.class");
    try (FSDataInputStream is = jarPath.getFileSystem(conf).open(jarPath)) {
      String md5 = DigestUtils.md5Hex(is);
      Assert.assertEquals("7ae4e24f0a4a72f83ab73fbb5c5f40be", md5);
    }
  }
}
