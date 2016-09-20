package spark.jobserver

import java.net.URL
import akka.actor.ActorRef
import akka.util.Timeout
import org.apache.spark.{SparkContext, SparkEnv}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.io.JobDAOActor
import spark.jobserver.util.{ContextURLClassLoader, JarUtils, LRUCache}

import scala.util.{Success, Failure}

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
      val jarFilePath = new java.io.File(jarPath).getAbsolutePath()
      sparkContext.addJar(jarFilePath) // Adds jar for remote executors
      loader.addURL(new URL("file:" + jarFilePath)) // Now jar added for local loader
      val constructor = JarUtils.loadClassOrObject[SparkJobBase](classPath, loader)
      JobJarInfo(constructor, classPath, jarFilePath)
    })
  }
}
