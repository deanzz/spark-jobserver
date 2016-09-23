package spark.jobserver

import java.io.{BufferedOutputStream, File, FileOutputStream, IOException}
import java.net.URL
import java.nio.file.{Files, Paths, StandardOpenOption}

import akka.actor.ActorRef
import akka.util.Timeout
import org.apache.spark.{SparkContext, SparkEnv}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.io.JobDAOActor
import spark.jobserver.util.{ContextURLClassLoader, JarUtils, LRUCache}

import scala.util.{Failure, Success}

case class JobJarInfo(constructor: () => SparkJobBase,
                      className: String,
                      jarFilePath: String)

/**
 * A cache for SparkJob classes.  A lot of times jobs are run repeatedly, and especially for low-latency
 * jobs, why retrieve the jar and load it every single time?
 */

class JobCache(maxEntries: Int, dao: ActorRef, sparkContext: SparkContext, loader: ContextURLClassLoader) {
  import scala.concurrent.duration._

  private val cache = new LRUCache[(String, DateTime, String), JobJarInfo](maxEntries)
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val daoAskTimeout: Timeout = Timeout(60 seconds)

  /**
   * Retrieves the given SparkJob class from the cache if it's there, otherwise use the DAO to retrieve it.
   * @param appName the appName under which the jar was uploaded
   * @param uploadTime the upload time for the version of the jar wanted
   * @param classPath the fully qualified name of the class/object to load
   */
  def getSparkJob(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo = {
    cache.get((appName, uploadTime, classPath), {
      import akka.pattern.ask
      import scala.concurrent.Await

      logger.info("Begin to get jar path for app {}, uploadTime {} from dao {}", appName,
        uploadTime, dao.path.toSerializationFormat)
      val jarPathReq = (dao ? JobDAOActor.GetJarPath(appName, uploadTime)).mapTo[JobDAOActor.JarPath]
      val jarPath = Await.result(jarPathReq, daoAskTimeout.duration).jarPath
      logger.info("End of get jar path for app {}, uploadTime {}, jarPath {}", appName, uploadTime, jarPath)
      val jarFile = Paths.get(jarPath)
      if (!Files.exists(jarFile)){
        logger.info("Local jar path {} not exist, fetch binary content from remote actor", jarPath)
        val jarBinaryReq = (dao ? JobDAOActor.GetBinaryJar(appName, uploadTime)).mapTo[JobDAOActor.BinaryJar]
        val binaryJar = Await.result(jarBinaryReq, daoAskTimeout.duration)
        logger.info("Writing {} bytes to file {}", binaryJar.jar.size, jarFile.toAbsolutePath.toString)
        try {
          if (!Files.exists(jarFile.getParent)){
            Files.createDirectories(jarFile.getParent)
          }
          Files.write(jarFile,binaryJar.jar)
        }catch {
          case e: IOException => logger.error("Write to path {} error {}", jarPath: Any, e)
        }
      }
      val jarFilePath = jarFile.toAbsolutePath.toString
      sparkContext.addJar(jarFilePath) // Adds jar for remote executors
      loader.addURL(new URL("file:" + jarFilePath)) // Now jar added for local loader
      val constructor = JarUtils.loadClassOrObject[SparkJobBase](classPath, loader)
      JobJarInfo(constructor, classPath, jarFilePath)
    })
  }
}
