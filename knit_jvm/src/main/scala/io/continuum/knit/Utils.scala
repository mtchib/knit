package io.continuum.knit
import java.io.File
import java.net.{InetAddress, URI, URISyntaxException, UnknownHostException}
import java.nio.file.FileSystem

import com.google.common.base.Objects
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, FileSystem, FileUtil, Path}
import org.apache.hadoop.fs
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{LocalResource, LocalResourceType, LocalResourceVisibility}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils}
import org.apache.log4j.Logger

import scala.collection.JavaConverters._

object Utils extends Logging {

  // add details of the local resource*
  def setUpLocalResource(resourcePath: Path, resource: LocalResource, archived: Boolean = false)(implicit conf: Configuration) = {

    val fileType = if (archived) LocalResourceType.ARCHIVE else LocalResourceType.FILE

    val jarStat = FileSystem.get(conf).getFileStatus(resourcePath)
    resource.setResource(ConverterUtils.getYarnUrlFromPath(resourcePath))
    resource.setSize(jarStat.getLen())
    resource.setTimestamp(jarStat.getModificationTime())
    resource.setType(fileType)
    resource.setVisibility(LocalResourceVisibility.PUBLIC)
  }

  //add the yarn jars to classpath
  def setUpEnv(env: collection.mutable.Map[String, String])(implicit conf: YarnConfiguration) = {
    val classPath = conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH: _*)
    for (c <- classPath) {
      Apps.addToEnvironment(env.asJava, Environment.CLASSPATH.name(),
        c.trim())
    }
    Apps.addToEnvironment(env.asJava,
      Environment.CLASSPATH.name(),
      Environment.PWD.$() + File.separator + "*")

  }

  def setDependencies()(implicit conf: YarnConfiguration) = {
    val fs = FileSystem.get(conf)
    val stagingDir = ".knitDeps"
    val stagingDirPath = new Path(sys.env("HDFS_KNIT_DIR"), stagingDir)

    // Staging directory is globally readable for now
    val STAGING_DIR_PERMISSION: FsPermission =
      FsPermission.createImmutable(Integer.parseInt("777", 8).toShort)
    FileSystem.mkdirs(fs, stagingDirPath, new FsPermission(STAGING_DIR_PERMISSION))

    val replicationFactor = sys.env("REPLICATION_FACTOR").toShort
    logger.info(s"Setting Replication Factor to: $replicationFactor")

    val jarDepPath = Seq(sys.env("KNIT_HOME")).mkString(File.separator)
    val KNIT_JAR = new File(jarDepPath, "knit-1.0-SNAPSHOT.jar").getAbsolutePath()
    logger.info(s"Attemping upload of $KNIT_JAR to $stagingDirPath")

    // upload all files to stagingDir
    List(KNIT_JAR).foreach {
      case (file) =>
        val p = getQualifiedLocalPath(Utils.resolveURI(file))
        copyFileToRemote(stagingDirPath, p, replicationFactor)
    }
  }

  def uploadFile(filePath: String)(implicit conf: YarnConfiguration) = {
    val fs = FileSystem.get(conf)
    val stagingDir = ".knitDeps"
    val stagingDirPath = new Path(sys.env("HDFS_KNIT_DIR"), stagingDir)
    val replicationFactor = sys.env("REPLICATION_FACTOR").toShort

    val FILE_PATH = new File(filePath).getAbsolutePath()
    logger.info(s"Attempting upload of $FILE_PATH to $stagingDirPath")

    // upload all files to stagingDir
    List(FILE_PATH).foreach {
      case (file) =>
        val p = getQualifiedLocalPath(Utils.resolveURI(file))
        copyFileToRemote(stagingDirPath, p, replicationFactor)
    }
  }

  def copyFileToRemote(destDir: Path, srcPath: Path,
                       replication: Short)(implicit conf: Configuration): Unit = {

    // App files are world-wide readable and owner writable -> rw-r--r--
    val APP_FILE_PERMISSION: FsPermission =
      FsPermission.createImmutable(Integer.parseInt("644", 8).toShort)

    val destFs = destDir.getFileSystem(conf)
    val srcFs = srcPath.getFileSystem(conf)
    var destPath = srcPath
    if (!compareFs(srcFs, destFs)) {
      destPath = new Path(destDir, srcPath.getName())
      logger.debug(s"Uploading resource $srcPath -> $destPath")
      FileUtil.copy(srcFs, srcPath, destFs, destPath, false, conf)
      destFs.setReplication(destPath, replication)
      destFs.setPermission(destPath, new FsPermission(APP_FILE_PERMISSION))
    } else {
      logger.info(s"Source and destination file systems are the same. Not copying $srcPath")
    }
  }
  /*
  * *
    * Given a local URI, resolve it and return a qualified local path that corresponds to the URI.
    * This is used for preparing local resources to be included in the container launch context.
    */
  private def getQualifiedLocalPath(localURI: URI)(implicit conf: Configuration): Path = {
    val qualifiedURI =
      if (localURI.getScheme == null) {
        // If not specified, assume this is in the local filesystem to keep the behavior
        // consistent with that of Hadoop
        new URI(FileSystem.getLocal(conf).makeQualified(new Path(localURI)).toString)
      } else {
        localURI
      }
    new Path(qualifiedURI)
  }

  private def compareFs(srcFs: FileSystem, destFs: FileSystem): Boolean = {
    val srcUri = srcFs.getUri()
    val dstUri = destFs.getUri()
    if (srcUri.getScheme() == null || srcUri.getScheme() != dstUri.getScheme()) {
      return false
    }
    var srcHost = srcUri.getHost()
    var dstHost = dstUri.getHost()

    // In HA or when using viewfs, the host part of the URI may not actually be a host, but the
    // name of the HDFS namespace. Those names won't resolve, so avoid even trying if they
    // match.
    if (srcHost != null && dstHost != null && srcHost != dstHost) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName()
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName()
      } catch {
        case e: UnknownHostException =>
          return false
      }
    }

    Objects.equal(srcHost, dstHost) && srcUri.getPort() == dstUri.getPort()

  }

  def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (uri.getFragment() != null) {
        val absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI()
        return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      }
    } catch {
      case e: URISyntaxException =>
    }
    new File(path).getAbsoluteFile().toURI()
  }
}
