package net.mbl.demo.hcfsjar;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

public class JarFilesystem extends FileSystem {

  @Override
  public URI getUri() {
    return null;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    URI uri = f.toUri();
    String purePath = uri.getPath();
    if (purePath == null) {
      purePath = uri.getSchemeSpecificPart();
    }

    if (purePath == null) {
      throw new IOException("wrong path uri, have neither path nor decodedSchemeSpecificPart");
    }

    if (purePath.startsWith("file:")) {
      purePath = purePath.substring(5);
    }
    f = new Path(uri.getScheme(), uri.getAuthority(), purePath);

    String jarAndEntryPath = f.toUri().getPath();
    if (jarAndEntryPath == null || !jarAndEntryPath.contains("jar!")) {
      throw new IOException("path have no jar!");
    }
    String[] jarAndEntryPair = jarAndEntryPath.split("!/");

    JarFile jarFile = new JarFile(jarAndEntryPair[0]);
    ZipEntry parentEntry = jarFile.getEntry(jarAndEntryPair[1]);
    if (parentEntry.isDirectory()) {
      throw new IOException("cannot open a dir");
    }

    return new FSDataInputStream(new JarEntryInputStream(jarFile.getInputStream(parentEntry)));
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    return null;
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    return null;
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return false;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return false;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    URI uri = f.toUri();
    String purePath = uri.getPath();
    if (purePath == null) {
      purePath = uri.getSchemeSpecificPart();
    }

    if (purePath == null) {
      return new FileStatus[0];
    }

    if (purePath.startsWith("file:")) {
      purePath = purePath.substring(5);
    }
    f = new Path(uri.getScheme(), uri.getAuthority(), purePath);

    String jarAndEntryPath = f.toUri().getPath();
    if (jarAndEntryPath == null || !jarAndEntryPath.contains("jar!")) {
      return new FileStatus[0];
    }
    String[] jarAndEntryPair = jarAndEntryPath.split("!/");

    JarFile jarFile = new JarFile(jarAndEntryPair[0]);
    ZipEntry parentEntry = jarFile.getEntry(jarAndEntryPair[1]);
    if (!parentEntry.isDirectory() && parentEntry.getCrc() != 0) {
      return new FileStatus[]{
          new FileStatus(parentEntry.getSize(), parentEntry.isDirectory(), 0, 0, 0, f)};
    }

    java.nio.file.Path parentPath = Paths.get(parentEntry.getName());
    List<FileStatus> fileStatusList = new LinkedList<>();
    Enumeration<JarEntry> entries = jarFile.entries();
    while (entries.hasMoreElements()) {
      JarEntry jarEntry = entries.nextElement();
      java.nio.file.Path path = Paths.get(jarEntry.getName());
      java.nio.file.Path parent = path.getParent();
      if (parentPath.equals(parent)) {
        Path childPath = new Path(f, path.getFileName().toString());
        fileStatusList
            .add(new FileStatus(jarEntry.getSize(), jarEntry.isDirectory(), 0, 0, 0, childPath));
      }
    }

    return fileStatusList.toArray(new FileStatus[fileStatusList.size()]);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
  }

  @Override
  public Path getWorkingDirectory() {
    return null;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return false;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    URI uri = f.toUri();
    String purePath = uri.getPath();
    if (purePath == null) {
      purePath = uri.getSchemeSpecificPart();
    }

    if (purePath == null) {
      throw new IOException("wrong path uri, have neither path nor decodedSchemeSpecificPart");
    }

    if (purePath.startsWith("file:")) {
      purePath = purePath.substring(5);
    }
    f = new Path(uri.getScheme(), uri.getAuthority(), purePath);

    String jarAndEntryPath = f.toUri().getPath();
    if (jarAndEntryPath == null || !jarAndEntryPath.contains("jar!")) {
      throw new IOException("path have no jar!");
    }
    String[] jarAndEntryPair = jarAndEntryPath.split("!/");

    JarFile jarFile = new JarFile(jarAndEntryPair[0]);
    ZipEntry parentEntry = jarFile.getEntry(jarAndEntryPair[1]);

    FileStatus fileStatus = new FileStatus(parentEntry.getSize(), parentEntry.isDirectory(), 0, 0,
        0, f);
    return fileStatus;
  }

  @Override
  public String getScheme() {
    return "jar";
  }
}
