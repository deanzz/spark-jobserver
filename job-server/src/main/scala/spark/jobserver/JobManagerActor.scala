package spark.jobserver

import java.io.File
import java.net.{URI, URL}
import java.util.concurrent.Executors._
import java.util.concurrent.atomic.AtomicInteger

import aco.jobserver.common.JobServerMessage._
import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{ActorRef, OneForOneStrategy, PoisonPill, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, UnreachableMember}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.joda.time.DateTime
import org.scalactic._
import spark.jobserver.api.{DataFileCache, JobEnvironment}
import spark.jobserver.context.{JobContainer, SparkContextFactory}
import spark.jobserver.io._
import spark.jobserver.util.{ContextURLClassLoader, SparkJobUtils}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import spark.jobserver.common.akka.InstrumentedActor

object JobManagerActor {

  // Messages
  case class Initialize(contextConfig: Config, resultActorOpt: Option[ActorRef],
                        dataFileActor: ActorRef)

  case class StartJob(appName: String, classPath: String, config: Config,
                      subscribedEvents: Set[Class[_]],
                      jobId: Option[String] = None, workerActor: Option[ActorRef] = None)

  case class KillJob(jobId: String)

  case class JobKilledException(jobId: String) extends Exception(s"Job $jobId killed")

  case class ContextTerminatedException(contextName: String)
    extends Exception(s"Unexpected termination of context $contextName")

  case object GetContextConfig

  case object SparkContextStatus

  case object GetSparkWebUIUrl

  case class DeleteData(name: String)

  // Results/Data
  case class ContextConfig(contextName: String, contextConfig: SparkConf, hadoopConfig: Configuration)

  case class Initialized(contextName: String, resultActor: ActorRef)

  case class InitError(t: Throwable)

  case class JobLoadingError(err: Throwable)

  case class SparkWebUIUrl(url: String)

  case object SparkContextAlive

  case object SparkContextDead

  case object NoSparkWebUI


  // Akka 2.2.x style actor props for actor creation
  def props(daoActor: ActorRef, clusterAddress: Option[String] = None): Props =
    Props(classOf[JobManagerActor], daoActor, clusterAddress)
}

/**
  * The JobManager actor supervises jobs running in a single SparkContext, as well as shared metadata.
  * It creates a SparkContext (or a StreamingContext etc. depending on the factory class)
  * It also creates and supervises a JobResultActor and JobStatusActor, although an existing JobResultActor
  * can be passed in as well.
  *
  * == contextConfig ==
  * {{{
  *  num-cpu-cores = 4         # Total # of CPU cores to allocate across the cluster
  *  memory-per-node = 512m    # -Xmx style memory string for total memory to use for executor on one node
  *  dependent-jar-uris = ["local://opt/foo/my-foo-lib.jar"]
  *                            # URIs for dependent jars to load for entire context
  *  context-factory = "spark.jobserver.context.DefaultSparkContextFactory"
  *  spark.mesos.coarse = true  # per-context, rather than per-job, resource allocation
  *  rdd-ttl = 24 h            # time-to-live for RDDs in a SparkContext.  Don't specify = forever
  *  is-adhoc = false          # true if context is ad-hoc context
  *  context.name = "sql"      # Name of context
  * }}}
  *
  * == global configuration ==
  * {{{
  *   spark {
  *     jobserver {
  *       max-jobs-per-context = 16      # Number of jobs that can be run simultaneously per context
  *     }
  *   }
  * }}}
  */
class JobManagerActor(daoActor: ActorRef, clusterAddressOpt: Option[String])
  extends InstrumentedActor {

  import CommonMessages._
  import JobManagerActor._
  import scala.concurrent.duration._
  import collection.JavaConverters._

  implicit val system = context.system
  val config = context.system.settings.config
  private val maxRunningJobs = SparkJobUtils.getMaxRunningJobs(config)
  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(maxRunningJobs))

  var jobContext: ContextLike = _
  var sparkEnv: SparkEnv = _

  private val currentRunningJobs = new AtomicInteger(0)

  // When the job cache retrieves a jar from the DAO, it also adds it to the SparkContext for distribution
  // to executors.  We do not want to add the same jar every time we start a new job, as that will cause
  // the executors to re-download the jar every time, and causes race conditions.

  private val jobCacheSize = Try(config.getInt("spark.job-cache.max-entries")).getOrElse(10000)
  //private val jobCacheEnabled = Try(config.getBoolean("spark.job-cache.enabled")).getOrElse(false)
  // Use Spark Context's built in classloader when SPARK-1230 is merged.
  private val jarLoader = new ContextURLClassLoader(Array[URL](), getClass.getClassLoader)

  // NOTE: Must be initialized after cluster joined
  private var contextConfig: Config = _
  private var contextName: String = _
  private var isAdHoc: Boolean = _
  private var statusActor: ActorRef = _
  protected var resultActor: ActorRef = _
  private var factory: SparkContextFactory = _
  private var remoteFileCache: RemoteFileCache = _

  // NOTE: Must be initialized after sparkContext is created
  private var jobCache: JobCache = _

  private val jobServerNamedObjects = new JobServerNamedObjects(context.system)
  private val errorEvents: Set[Class[_]] = Set(classOf[JobErroredOut], classOf[JobValidationFailed])
  private val asyncEvents = Set(classOf[JobStarted]) ++ errorEvents
  private val startJobSucceedTpl =
    """
      |{"status":"%s","classPath":"%s","startTime":"%s",
      |"context":"%s","jobId":"%s","result":"%s","duration":"%s"}
    """.stripMargin
  private val cluster = Cluster(context.system)
  //private val clusterAddress = clusterAddressOpt.flatMap(s => Some(AddressFromURIString.parse(s)))
  private val AbstractPBJobExceptionKey = "AbstractPBJob_"
 /* private val enableK8sCheck = Try(config.getBoolean("kubernetes.check.enable")).getOrElse(false)
  private val k8sClient = new K8sHttpClient(config)
  private val acoMonitor: Option[ActorRef] = Some(context.system.actorOf(ClusterSingletonProxy.props(
    singletonManagerPath = "/user/jobserver-monitor",
    settings = ClusterSingletonProxySettings(context.system).withRole("scheduler")),
    name = "jobserver-monitor-proxy"))*/

  private def getEnvironment(_jobId: String): JobEnvironment = {
    val _contextCfg = contextConfig
    new JobEnvironment with DataFileCache {
      def jobId: String = _jobId

      def namedObjects: NamedObjects = jobServerNamedObjects

      def contextConfig: Config = _contextCfg

      def getDataFile(dataFile: String): File = {
        remoteFileCache.getDataFile(dataFile)
      }
    }
  }

  override def preStart(): Unit = {
    context.watch(self)
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[UnreachableMember])
    logger.info("JobManagerActor preStart")
  }

  override def postStop() {
    logger.info("JobManagerActor Shutting down SparkContext {}", contextName)
    cluster.unsubscribe(self)
    Option(jobContext).foreach(_.stop())
  }

  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case e: Exception =>
        logger.error(s"JobManagerActor OneForOneStrategy got error, " +
          s"${e.getMessage}", e)
        Escalate
    }

  // Handle external kill events (e.g. killed via YARN)
  private def sparkListener = {
    new SparkListener() {
      override def onApplicationEnd(event: SparkListenerApplicationEnd) {
        logger.info(s"Got Spark Application end event, " +
          s"context [$contextName] is killed due to exception.")
        /*if (enableK8sCheck) {
          Try(k8sClient.podLog(contextName)) match {
            case Success(log) =>
              logger.error(s"error log from k8s:\n$log")
              acoMonitor.foreach(m => m ! ContextTerminated(contextName, log))
            case Failure(e) =>
              val errMsg = e match {
                case _: TimeoutException => "Connect kubernetes API timeout!!"
                case _ => s"${e.getMessage}\n${e.getStackTrace.map(_.toString).mkString("\n")}"
              }
              acoMonitor.foreach(m => m ! ContextTerminated(contextName, errMsg))
          }
        } else {
          acoMonitor.foreach(m => m ! ContextTerminated(contextName,
            "No error log when kubernetes.check.enable is false"))
        }*/
        self ! PoisonPill
      }
    }
  }

  def wrappedReceive: Receive = {
    case UnreachableMember(member) =>
      logger.info("Member detected as unreachable: {}", member.address)
      if (member.hasRole("supervisor")) {
        logger.info("context kill self: {}", self)
        self ! PoisonPill
      }

    case Initialize(ctxConfig, resOpt, dataManagerActor) =>
      if (statusActor == null) {
        contextConfig = ctxConfig
        logger.info("Starting context with config:\n" + contextConfig.root.render)
        contextName = contextConfig.getString("context.name")
        isAdHoc = Try(contextConfig.getBoolean("is-adhoc")).getOrElse(false)
        statusActor = context.actorOf(JobStatusActor.props(daoActor))
        resultActor = resOpt.getOrElse(context.actorOf(Props[JobResultActor]))
        remoteFileCache = new RemoteFileCache(self, dataManagerActor)
        try {
          // Load side jars first in case the ContextFactory comes from it
          getSideJars(contextConfig).foreach { jarUri =>
            jarLoader.addURL(new URL(convertJarUriSparkToJava(jarUri)))
          }
          factory = getContextFactory()
          jobContext = factory.makeContext(config, contextConfig, contextName)
          jobContext.sparkContext.addSparkListener(sparkListener)
          sparkEnv = SparkEnv.get
          jobCache = new JobCacheImpl(jobCacheSize, daoActor, jobContext.sparkContext, jarLoader)
          getSideJars(contextConfig).foreach { jarUri => jobContext.sparkContext.addJar(jarUri) }
          sender ! Initialized(contextName, resultActor)
        } catch {
          case t: Throwable =>
            logger.error("Failed to create context " + contextName + ", shutting down actor", t)
            sender ! InitError(t)
            self ! PoisonPill
        }
      } else {
        logger.info(s"statusActor is not null, it's a rejoined JobManagerActor[$contextName], " +
          "do not need to be initialized")
        sender ! Initialized(contextName, resultActor)
      }


    case StartJobWrapper(appName, classPath, jobId, parameters) =>
      val requestWorker = sender()
      val postedJobConfig = ConfigFactory.parseString(parameters)
      val jobConfig = postedJobConfig.withFallback(config).resolve()

      self ! StartJob(appName, classPath, jobConfig, asyncEvents, Some(jobId), Some(requestWorker))

    case StartJob(appName, classPath, jobConfig, events, jobId, workerActor) => {
      logger.info(s"StartJob, jobId [$jobId], classPath [$classPath], workerActor [$workerActor]")
      val loadedJars = jarLoader.getURLs
      getSideJars(jobConfig).foreach { jarUri =>
        val jarToLoad = new URL(convertJarUriSparkToJava(jarUri))
        if (!loadedJars.contains(jarToLoad)) {
          logger.info("Adding {} to Current Job Class path", jarUri)
          jarLoader.addURL(new URL(convertJarUriSparkToJava(jarUri)))
          jobContext.sparkContext.addJar(jarUri)
        }
      }
      startJobInternal(appName, classPath, jobConfig, events, jobContext, sparkEnv, jobId, workerActor)
    }

    case KillJob(jobId: String) => {
      jobContext.sparkContext.cancelJobGroup(jobId)
      val resp = JobKilled(jobId, DateTime.now())
      statusActor ! resp
      sender ! resp
    }

    case SparkContextStatus => {
      if (jobContext.sparkContext == null) {
        sender ! SparkContextDead
      } else {
        try {
          jobContext.sparkContext.getSchedulingMode
          sender ! SparkContextAlive
        } catch {
          case e: Exception => {
            logger.error("SparkContext does not exist!")
            sender ! SparkContextDead
          }
        }
      }
    }

    case GetContextConfig => {
      if (jobContext.sparkContext == null) {
        sender ! SparkContextDead
      } else {
        try {
          val conf: SparkConf = jobContext.sparkContext.getConf
          val hadoopConf: Configuration = jobContext.sparkContext.hadoopConfiguration
          sender ! ContextConfig(jobContext.sparkContext.appName, conf, hadoopConf)
        } catch {
          case e: Exception => {
            logger.error("SparkContext does not exist!")
            sender ! SparkContextDead
          }
        }
      }
    }

    case GetSparkWebUIUrl => {
      if (jobContext.sparkContext == null) {
        sender ! SparkContextDead
      } else {
        try {
          val webUiUrl = jobContext.sparkContext.uiWebUrl
          val msg = if (webUiUrl.isDefined) {
            SparkWebUIUrl(webUiUrl.get)
          } else {
            NoSparkWebUI
          }
          sender ! msg
        } catch {
          case e: Exception => {
            logger.error("SparkContext does not exist!")
            sender ! SparkContextDead
          }
        }
      }
    }

    case DeleteData(name: String) => {
      remoteFileCache.deleteDataFile(name)
    }
  }

  def startJobInternal(appName: String,
                       classPath: String,
                       jobConfig: Config,
                       events: Set[Class[_]],
                       jobContext: ContextLike,
                       sparkEnv: SparkEnv,
                       jobIdOpt: Option[String],
                       workerActor: Option[ActorRef]): Option[Future[Any]] = {
    import akka.pattern.ask
    import akka.util.Timeout
    import spark.jobserver.context._

    import scala.concurrent.Await
    import scala.concurrent.duration._

    def failed(msg: Any): Option[Future[Any]] = {
      sender ! msg
      postEachJob()
      None
    }

    val daoAskTimeout = Timeout(3 seconds)
    // TODO: refactor so we don't need Await, instead flatmap into more futures
    val resp = Await.result(
      (daoActor ? JobDAOActor.GetLastUploadTimeAndType(appName)) (daoAskTimeout).
        mapTo[JobDAOActor.LastUploadTimeAndType],
      daoAskTimeout.duration)

    val jobId = jobIdOpt.getOrElse(java.util.UUID.randomUUID().toString)

    val lastUploadTimeAndType = resp.uploadTimeAndType
    if (lastUploadTimeAndType.isEmpty) {
      if (workerActor.isDefined) {
        notifyStartJobFailedToAco(workerActor.get, jobId, NoSuchApplication, Some(appName))
      }
      return failed(NoSuchApplication)
    }
    val (lastUploadTime, binaryType) = lastUploadTimeAndType.get

    val jobContainer = factory.loadAndValidateJob(appName, lastUploadTime,
      classPath, jobCache) match {
      case Good(container) => container
      case Bad(JobClassNotFound) =>
        if (workerActor.isDefined) {
          notifyStartJobFailedToAco(workerActor.get, jobId, NoSuchClass, Some(classPath))
        }
        return failed(NoSuchClass)
      case Bad(JobWrongType) =>
        if (workerActor.isDefined) {
          notifyStartJobFailedToAco(workerActor.get, jobId, WrongJobType)
        }
        return failed(WrongJobType)
      case Bad(JobLoadError(ex)) =>
        if (workerActor.isDefined) {
          notifyStartJobFailedToAco(workerActor.get, jobId, JobLoadError(ex))
        }
        return failed(JobLoadingError(ex))
    }

    // Automatically subscribe the sender to events so it starts getting them right away
    resultActor ! Subscribe(jobId, sender, events)
    statusActor ! Subscribe(jobId, sender, events)

    val binInfo = BinaryInfo(appName, binaryType, lastUploadTime)
    val jobInfo = JobInfo(jobId, contextName, binInfo, classPath, DateTime.now(), None, None)

    Some(getJobFuture(jobContainer, jobInfo, jobConfig, sender, jobContext, sparkEnv, workerActor.get))
  }

  private def getJobFuture(container: JobContainer,
                           jobInfo: JobInfo,
                           jobConfig: Config,
                           subscriber: ActorRef,
                           jobContext: ContextLike,
                           sparkEnv: SparkEnv,
                           workerActor: ActorRef): Future[Any] = {

    val jobId = jobInfo.jobId
    logger.info("Starting Spark job {} [{}]...", jobId: Any, jobInfo.classPath)

    // Atomically increment the number of currently running jobs. If the old value already exceeded the
    // limit, decrement it back, send an error message to the sender, and return a dummy future with
    // nothing in it.
    if (currentRunningJobs.getAndIncrement() >= maxRunningJobs) {
      currentRunningJobs.decrementAndGet()
      val msg = NoJobSlotsAvailable(maxRunningJobs)
      sender ! msg
      notifyStartJobFailedToAco(workerActor, jobId, msg, Some(contextName))
      return Future[Any](None)(context.dispatcher)
    }

    Future {
      org.slf4j.MDC.put("jobId", jobId)
      logger.info("Starting job future thread")
      try {
        // Need to re-set the SparkEnv because it's thread-local and the Future runs on a diff thread
        SparkEnv.set(sparkEnv)

        // Use the Spark driver's class loader as it knows about all our jars already
        // NOTE: This may not even be necessary if we set the driver ActorSystem classloader correctly
        Thread.currentThread.setContextClassLoader(jarLoader)
        val job = container.getSparkJob
        statusActor ! JobStatusActor.JobInit(jobInfo)
        val jobC = jobContext.asInstanceOf[job.C]
        val jobEnv = getEnvironment(jobId)
        job.validate(jobC, jobEnv, jobConfig) match {
          case Bad(reasons) =>
            val err = new Throwable(reasons.toString)
            val msg = JobValidationFailed(jobId, DateTime.now(), err)
            statusActor ! msg
            notifyStartJobFailedToAco(workerActor, jobId, msg)
            throw err
          case Good(jobData) =>
            val sc = jobContext.sparkContext
            sc.setJobGroup(jobId, s"Job group for $jobId and spark context ${sc.applicationId}", true)
            statusActor ! JobStarted(jobId, jobInfo)

            val resp = startJobSucceedTpl.format(
              JobStatus.Started, jobInfo.classPath, jobInfo.startTime, jobInfo.contextName,
              jobId, "", getJobDurationString(jobInfo.jobLengthMillis))
            workerActor ! StartJobSucceedWrapper(jobId, resp)

            try {
              val result = job.runJob(jobC, jobEnv, jobData)
              logger.info(s"Running job [$jobId] succeed. result is \n${result.toString}")
              workerActor ! RunningJobResult(jobId, succeed = true, result.toString)
              result
            } catch {
              case e: Throwable =>
                val msg = e.getMessage
                if (msg.startsWith(AbstractPBJobExceptionKey)) {
                  val errJsonStart = msg.indexOf(AbstractPBJobExceptionKey) + AbstractPBJobExceptionKey.length
                  val errJson = msg.substring(errJsonStart)
                  workerActor ! RunningJobResult(jobId, succeed = false, errJson)
                } else {
                  logger.error(s"Init job error, ${e.getMessage}", e)
                  val senderInfo = jobConfig.getConfig("senderInfo")
                  val jobServerSchema = senderInfo.getString("schema")
                  val jobServerHost = senderInfo.getString("host")
                  val jobServerPort = senderInfo.getInt("port")
                  val jobServerRole = senderInfo.getString("role")
                  val jobServerAuthType = Try(senderInfo.getString("authType")).getOrElse("")
                  val jobServerCredential = Try(senderInfo.getString("credential")).getOrElse("")
                  val errMsg = e.getMessage
                  val errClass = e.getClass.getName
                  val errStack = e.getStackTrace.map(_.toString).mkString("\n")
                  val graphId = senderInfo.getString("graphId")
                  val nodeId = senderInfo.getString("nodeId")
                  val topicId = senderInfo.getString("topicId")

                  workerActor ! InitJobFailed(jobId, jobServerSchema, jobServerHost, jobServerPort,
                    jobServerRole, jobServerAuthType, jobServerCredential, errMsg, errClass,
                    errStack, graphId, nodeId, topicId)
                }
                throw e
            }
        }
      } catch {
        case e: Throwable =>
          logger.error(s"Submit job error, ${e.getMessage}", e)
          notifyStartJobFailedToAco(workerActor, jobId, e)
          throw e
      } finally {
        org.slf4j.MDC.remove("jobId")
      }

    }(executionContext).andThen {
      case Success(result: Any) =>
        // TODO: If the result is Stream[_] and this is running with context-per-jvm=true configuration
        // serializing a Stream[_] blob across process boundaries is not desirable.
        // In that scenario an enhancement is required here to chunk stream results back.
        // Something like ChunkedJobResultStart, ChunkJobResultMessage, and ChunkJobResultEnd messages
        // might be a better way to send results back and then on the other side use chunked encoding
        // transfer to send the chunks back. Alternatively the stream could be persisted here to HDFS
        // and the streamed out of InputStream on the other side.
        // Either way an enhancement would be required here to make Stream[_] responses work
        // with context-per-jvm=true configuration
        resultActor ! JobResult(jobId, result)
        statusActor ! JobFinished(jobId, DateTime.now())
      case Failure(error: Throwable) =>
        logger.error(s"Got job error, ${error.getMessage}", error)
        // Wrapping the error inside a RuntimeException to handle the case of throwing custom exceptions.
        val wrappedError = wrapInRuntimeException(error)
        // If and only if job validation fails, JobErroredOut message is dropped silently in JobStatusActor.
        val msg = JobErroredOut(jobId, DateTime.now(), wrappedError)
        statusActor ! msg
        logger.error("Exception from job " + jobId + ": ", error)
    }(executionContext).andThen {
      case _ =>
        // Make sure to decrement the count of running jobs when a job finishes, in both success and failure
        // cases.
        currentRunningJobs.getAndDecrement()
        resultActor ! Unsubscribe(jobId, subscriber)
        statusActor ! Unsubscribe(jobId, subscriber)
        postEachJob()
    }(executionContext)
  }

  // Wraps a Throwable object into a RuntimeException. This is useful in case
  // a custom exception is thrown. Currently, throwing a custom exception doesn't
  // work and this is a workaround to wrap it into a standard exception.
  protected def wrapInRuntimeException(t: Throwable): RuntimeException = {
    val cause: Throwable = getRootCause(t)
    val e: RuntimeException = new RuntimeException("%s: %s"
      .format(cause.getClass().getName(), cause.getMessage))
    e.setStackTrace(cause.getStackTrace())
    return e
  }

  // Gets the very first exception that caused the current exception to be thrown.
  protected def getRootCause(t: Throwable): Throwable = {
    var result: Throwable = t
    var cause: Throwable = result.getCause()
    while (cause != null && (result != cause)) {
      result = cause
      cause = result.getCause()
    }
    return result
  }

  // Use our classloader and a factory to create the SparkContext.  This ensures the SparkContext will use
  // our class loader when it spins off threads, and ensures SparkContext can find the job and dependent jars
  // when doing serialization, for example.
  def getContextFactory(): SparkContextFactory = {
    val factoryClassName = contextConfig.getString("context-factory")
    val factoryClass = jarLoader.loadClass(factoryClassName)
    val factory = factoryClass.newInstance.asInstanceOf[SparkContextFactory]
    Thread.currentThread.setContextClassLoader(jarLoader)
    factory
  }

  // This method should be called after each job is succeeded or failed
  private def postEachJob() {
    // Delete myself after each adhoc job
    if (isAdHoc) self ! PoisonPill
  }

  // Protocol like "local" is supported in Spark for Jar loading, but not supported in Java.
  // This method helps convert those Spark URI to those supported by Java.
  // "local" URIs means that the jar must be present on each job server node at the path,
  // as well as on every Spark worker node at the path.
  // For the job server, convert the local to a local file: URI since Java URI doesn't understand local:
  private def convertJarUriSparkToJava(jarUri: String): String = {
    val uri = new URI(jarUri)
    uri.getScheme match {
      case "local" => "file://" + uri.getPath
      case _ => jarUri
    }
  }

  // "Side jars" are jars besides the main job jar that are needed for running the job.
  // They are loaded from the context/job config.
  // Each one should be an URL (http, ftp, hdfs, local, or file). local URLs are local files
  // present on every node, whereas file:// will be assumed only present on driver node
  private def getSideJars(config: Config): Seq[String] =
    Try(config.getStringList("dependent-jar-uris").asScala.toSeq).
      orElse(Try(config.getString("dependent-jar-uris").split(",").toSeq)).getOrElse(Nil)

  private def getJobDurationString(duration: Option[Long]): String =
    duration.map { ms => ms / 1000.0 + " secs" }.getOrElse("Job not done yet")

  def formatException(t: Throwable): Any = {
    s"""{"message":"${t.getMessage}","errorClass":"${t.getClass.getName}",
       |"stack":"${ErrorData.getStackTrace(t)}"}""".stripMargin
  }

  def errResp(errMsg: String): String = {
    s"""{"status":"${JobStatus.Error}","result":"$errMsg"}"""
  }

  def errResp(t: Throwable, status: String): String = {
    s"""{"status":"$status","result":${formatException(t)}}"""
  }

  def errResp(errMsg: String, status: String): String = {
    s"""{"status":"$status","result":"$errMsg"}"""
  }

  def errResp(jobId: String, t: Throwable, status: String): String = {
    s"""{"jobId":"$jobId","status":"$status","result":${formatException(t)}}"""
  }

  def notifyStartJobFailedToAco(workerActor: ActorRef, jobId: String,
                                errMsg: Any, errMsgArg: Option[String] = None): Unit = {
    val resp = errMsg match {
      case NoJobSlotsAvailable(maxJobSlots) =>
        val errorMsg = "Too many running jobs (" + maxJobSlots.toString +
          ") for job context '" + errMsgArg.getOrElse("") + "'"
        errResp(errorMsg, "NO SLOTS AVAILABLE")
      case JobValidationFailed(_, _, ex) => errResp(ex, "VALIDATION FAILED")
      case JobErroredOut(jobId, _, ex) => errResp(jobId, ex, JobStatus.Error)
      case NoSuchApplication =>
        val msg = "appName " + errMsgArg.getOrElse("") + " not found"
        errResp(msg)
      case NoSuchClass =>
        val msg = "classPath " + errMsgArg.getOrElse("") + " not found"
        errResp(msg)
      case WrongJobType => errResp("Invalid job type for this context")
      case JobLoadingError(ex) => errResp(ex, "JOB LOADING FAILED")
      //case ContextInitError(ex) => errResp(ex, "CONTEXT INIT FAILED")
      case e: Throwable => errResp(e, "ERROR")
      case other => errResp(other.toString)
    }
    workerActor ! StartJobFailedWrapper(jobId, resp)
  }

}
