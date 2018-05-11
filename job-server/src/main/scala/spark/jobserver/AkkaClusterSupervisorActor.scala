package spark.jobserver

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import aco.jobserver.common.JobServerMessage._
import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberUp}
import akka.cluster.singleton.{ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.util.Timeout
import akka.pattern.ask
import com.typesafe.config.{Config, ConfigFactory}
import spark.jobserver.util.SparkJobUtils

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.sys.process._
import spark.jobserver.common.akka.InstrumentedActor

import scala.concurrent.{Await, TimeoutException}
import akka.pattern.gracefulStop
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.JobManagerActor.{GetSparkWebUIUrl, NoSparkWebUI, SparkContextDead, SparkWebUIUrl}
import spark.jobserver.clients.kubernetes.K8sHttpClient
import spark.jobserver.io.JobDAOActor.CleanContextJobInfos

/**
  * The AkkaClusterSupervisorActor launches Spark Contexts as external processes
  * that connect back with the master node via Akka Cluster.
  *
  * Currently, when the Supervisor gets a MemberUp message from another actor,
  * it is assumed to be one starting up, and it will be asked to identify itself,
  * and then the Supervisor will try to initialize it.
  *
  * See the [[LocalContextSupervisorActor]] for normal config options.  Here are ones
  * specific to this class.
  *
  * ==Configuration==
  * {{{
  *   deploy {
  *     manager-start-cmd = "./manager_start.sh"
  *   }
  * }}}
  */
class AkkaClusterSupervisorActor(daoActor: ActorRef, dataManagerActor: ActorRef)
  extends InstrumentedActor {

  import ContextSupervisor._
  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  implicit val system = context.system
  private val config = context.system.settings.config
  private val defaultContextConfig = config.getConfig("spark.context-settings")
  private val contextInitTimeout = config.getDuration("spark.context-settings.context-init-timeout",
    TimeUnit.SECONDS)
  private val contextTimeout = SparkJobUtils.getContextCreationTimeout(config)
  private val contextDeletionTimeout = SparkJobUtils.getContextDeletionTimeout(config)
  private val managerStartCommand = config.getString("deploy.manager-start-cmd")
  private val enableK8sCheck = Try(config.getBoolean("kubernetes.check.enable")).getOrElse(false)
  private val k8sClient = new K8sHttpClient(config)

  import context.dispatcher

  //actor name -> (context isadhoc, success callback, failure callback)
  //TODO: try to pass this state to the jobManager at start instead of having to track
  //extra state.  What happens if the WebApi process dies before the forked process
  //starts up?  Then it never gets initialized, and this state disappears.
  private val contextInitInfos = mutable.HashMap.empty[String,
    (Config, Boolean, ActorRef => Unit, Throwable => Unit)]

  // actor name -> (JobManagerActor ref, ResultActor ref)
  private val contexts = mutable.HashMap.empty[String, (ActorRef, ActorRef)]

  private val cluster = Cluster(context.system)
  private val selfAddress = cluster.selfAddress

  // This is for capturing results for ad-hoc jobs. Otherwise when ad-hoc job dies, resultActor also dies,
  // and there is no way to retrieve results.
  private val globalResultActor = context.actorOf(Props[JobResultActor], "global-result-actor")
  private val acoMonitor: Option[ActorRef] = Some(context.system.actorOf(ClusterSingletonProxy.props(
    singletonManagerPath = "/user/jobserver-monitor",
    settings = ClusterSingletonProxySettings(context.system).withRole("scheduler")),
    name = "jobserver-monitor-proxy"))

  private var jobServerRole: Option[String] = None
  // private val
  logger.info("AkkaClusterSupervisor initialized on {}", selfAddress)


  override def preStart(): Unit = {
    //cluster.join(selfAddress)
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent])
    logger.info("AkkaClusterSupervisorActor preStart")
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    cluster.leave(self.path.address)
    logger.info("AkkaClusterSupervisorActor postStop")
  }

  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case e: Exception =>
        logger.error(s"AkkaClusterSupervisorActor OneForOneStrategy got error from children, " +
          s"${e.getMessage}", e)
        Escalate
    }

  def wrappedReceive: Receive = {
    case MemberUp(member) =>
      logger.info(s"Member is up: roles ${member.roles}, address ${member.address}")
      if (member.hasRole("manager")) {
        val memberActors = RootActorPath(member.address) / "user" / "*"
        context.actorSelection(memberActors) ! Identify(memberActors)
      }

    case ActorIdentity(memberActors, actorRefOpt) =>
      logger.info(s"Got ActorIdentity [$memberActors], [$actorRefOpt]")
      actorRefOpt.foreach { actorRef =>
        val actorName = actorRef.path.name
        if (actorName.startsWith("jobManager")) {
          logger.info("Received identify response, attempting to initialize context at {}", memberActors)
          (for {(contextConfig, isAdHoc, successFunc, failureFunc) <- contextInitInfos.remove(actorName)}
            yield {
              initContext(contextConfig, actorName,
                actorRef, contextInitTimeout)(isAdHoc, successFunc, failureFunc)
            }).getOrElse({
            logger.warn("No initialization or callback found for jobManager actor {}", actorRef.path)
            actorRef ! PoisonPill
          })
        }
      }

    case AddContextsFromConfig =>
      addContextsFromConfig(config)

    case ListContexts =>
      sender ! contexts.keys.toSeq

    case AddContextWrapper(name, operatorId, parameters, operation) =>
      logger.info(s"Start AddContextWrapper ($name)")
      val requestWorker = sender()

      def create(): Unit = {
        val paramMap = parameters2Map(parameters)
        val contextConfig = ConfigFactory.parseMap(paramMap.asJava)
        self ! AddContext(name, contextConfig, Some(requestWorker), Some(operatorId), Some(operation))
      }

      if (enableK8sCheck) {
        Try(k8sClient.podStatus(name)) match {
          case Success(res) =>
            res match {
              case "NotFound" =>
                create()
              case other =>
                logger.info(s"The status of context [$name] in kubernetes is $other")
                contexts.get(name) match {
                  case Some(ctx) =>
                    val succeed = AddContextSucceedWrapper(name, ctx._1.path.toString,
                      operatorId, s"context $name exists", operation)
                    requestWorker ! succeed
                    acoMonitor.get ! succeed
                  case _ =>
                    val failed = AddContextFailedWrapper(name,
                      "Kubernetes error,Inconsistent jobserver and kubernetes status", operatorId, operation)
                    requestWorker ! failed
                    acoMonitor.get ! failed
                }
            }
          case Failure(e) =>
            logger.error(s"AddContextWrapper error, ${e.getMessage}", e)
            e match {
              case _: TimeoutException =>
                val failed = AddContextFailedWrapper(name,
                  s"Kubernetes error, Connect kubernetes API timeout!!",
                  operatorId, operation)
                requestWorker ! failed
                acoMonitor.get ! failed
              case _ =>
                val err = s"${e.getMessage}\n${e.getStackTrace.map(_.toString).mkString("\n")}"
                val failed = AddContextFailedWrapper(name, s"Kubernetes error, $err", operatorId, operation)
                requestWorker ! failed
                acoMonitor.get ! failed
            }
        }
      } else {
        create()
      }


    case AddContext(name, contextConfig, workerActor, operatorId, operation) =>
      val originator = sender()
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      // TODO(velvia): This check is not atomic because contexts is only populated
      // after SparkContext successfully created!  See
      // https://github.com/spark-jobserver/spark-jobserver/issues/349
      if (contexts contains name) {
        originator ! ContextAlreadyExists
        if (!enableK8sCheck && workerActor.isDefined) {
          val succeed = AddContextSucceedWrapper(name, contexts(name)._1.path.toString,
            operatorId.get, s"context $name exists", operation.get)
          workerActor.get ! succeed
          acoMonitor.get ! succeed
        }
      } else {
        startContext(name, mergedConfig, false) { ref =>
          originator ! ContextInitialized
          if (workerActor.isDefined) {
            val succeed = AddContextSucceedWrapper(name, ref.path.toString,
              operatorId.get, "SUCCESS", operation.get)
            workerActor.get ! succeed
            acoMonitor.get ! succeed
          }
        } { err =>
          originator ! ContextInitError(err)
          if (workerActor.isDefined) {
            val errMsg = s"${err.getMessage}\n${err.getStackTrace.map(t => t.toString).mkString("\n")}"
            workerActor.get ! AddContextFailedWrapper(name, errMsg, operatorId.get, operation.get)
          }
        }
      }

    case StartAdHocContext(classPath, contextConfig) =>
      val originator = sender
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      val userNamePrefix = Try(mergedConfig.getString(SparkJobUtils.SPARK_PROXY_USER_PARAM))
        .map(SparkJobUtils.userNamePrefix(_)).getOrElse("")
      var contextName = ""
      do {
        contextName = userNamePrefix + java.util.UUID.randomUUID().toString().take(8) + "-" + classPath
      } while (contexts contains contextName)
      // TODO(velvia): Make the check above atomic.  See
      // https://github.com/spark-jobserver/spark-jobserver/issues/349

      startContext(contextName, mergedConfig, true) { ref =>
        originator ! contexts(contextName)
      } { err =>
        originator ! ContextInitError(err)
      }

    case GetResultActor(name) =>
      sender ! contexts.get(name).map(_._2).getOrElse(globalResultActor)

    case GetContext(name) =>
      if (contexts contains name) {
        sender ! contexts(name)
      } else {
        sender ! NoSuchContext
      }

    case GetSparkWebUI(name) =>
      contexts.get(name) match {
        case Some((actor, _)) =>
          val future = (actor ? GetSparkWebUIUrl) (contextTimeout.seconds)
          val originator = sender
          future.collect {
            case SparkWebUIUrl(webUi) => originator ! WebUIForContext(name, Some(webUi))
            case NoSparkWebUI => originator ! WebUIForContext(name, None)
            case SparkContextDead =>
              logger.info("SparkContext {} is dead", name)
              originator ! NoSuchContext
          }
        case _ => sender ! NoSuchContext
      }

    case StopContext(name) =>
      if (contexts contains name) {
        logger.info("Shutting down context {}", name)
        val contextActorRef = contexts(name)._1
        cluster.down(contextActorRef.path.address)
        try {
          val stoppedCtx = gracefulStop(contexts(name)._1, contextDeletionTimeout seconds)
          Await.result(stoppedCtx, contextDeletionTimeout + 1 seconds)
          sender ! ContextStopped
        }
        catch {
          case err: Exception => sender ! ContextStopError(err)
        }
      } else {
        sender ! NoSuchContext
      }

    case Terminated(actorRef) =>
      val name: String = actorRef.path.name
      logger.info("Actor terminated: {}", name)
      for ((name, _) <- contexts.find(_._2._1 == actorRef)) {
        contexts.remove(name)
        daoActor ! CleanContextJobInfos(name, DateTime.now())
        if(enableK8sCheck){
          Try(k8sClient.podLog(name)) match {
            case Success(log) =>
              acoMonitor.get ! ContextTerminated(name, jobServerRole.getOrElse(""), log)
            case Failure(e) =>
              val errMsg = e match {
                case _: TimeoutException => "Connect kubernetes API timeout!!"
                case _ => s"${e.getMessage}\n${e.getStackTrace.map(_.toString).mkString("\n")}"
              }
              acoMonitor.get ! ContextTerminated(name, jobServerRole.getOrElse(""), errMsg)
          }
        } else {
          acoMonitor.get ! ContextTerminated(name, jobServerRole.getOrElse(""),
            "No error log when kubernetes.check.enable is false")
        }
      }
      cluster.down(actorRef.path.address)

    case SetJobServerRole(role) =>
      jobServerRole = Some(role)
      sender() ! SetJobServerRoleAck

  }

  private def initContext(contextConfig: Config,
                          actorName: String,
                          ref: ActorRef,
                          timeoutSecs: Long = 1)
                         (isAdHoc: Boolean,
                          successFunc: ActorRef => Unit,
                          failureFunc: Throwable => Unit): Unit = {
    import akka.pattern.ask

    val resultActor = if (isAdHoc) globalResultActor else context.actorOf(Props(classOf[JobResultActor]))
    (ref ? JobManagerActor.Initialize(
      contextConfig, Some(resultActor), dataManagerActor)) (Timeout(timeoutSecs.second)).onComplete {
      case Failure(e: Exception) =>
        logger.info("Failed to send initialize message to context " + ref, e)
        cluster.down(ref.path.address)
        ref ! PoisonPill
        failureFunc(e)
      case Success(JobManagerActor.InitError(t)) =>
        logger.info("Failed to initialize context " + ref, t)
        cluster.down(ref.path.address)
        ref ! PoisonPill
        failureFunc(t)
      case Success(JobManagerActor.Initialized(ctxName, resActor)) =>
        logger.info("SparkContext {} joined", ctxName)
        contexts(ctxName) = (ref, resActor)
        context.watch(ref)
        successFunc(ref)
      case _ => logger.info("Failed for unknown reason.")
        cluster.down(ref.path.address)
        ref ! PoisonPill
        failureFunc(new RuntimeException("Failed for unknown reason."))
    }
  }

  private def startContext(name: String, contextConfig: Config, isAdHoc: Boolean)
                          (successFunc: ActorRef => Unit)(failureFunc: Throwable => Unit): Unit = {
    require(!(contexts contains name), "There is already a context named " + name)
    val contextActorName = s"jobManager-${java.util.UUID.randomUUID().toString.substring(16)}__$name"

    logger.info("Starting context with actor name {}", contextActorName)

    val master = Try(config.getString("spark.master")).toOption.getOrElse("local[4]")
    val deployMode = Try(config.getString("spark.submit.deployMode")).toOption.getOrElse("client")
    val isKubernetesMode = master.trim.startsWith("k8s")

    // Create a temporary dir, preferably in the LOG_DIR
    val encodedContextName = java.net.URLEncoder.encode(name, "UTF-8")
    val contextDir = Option(System.getProperty("LOG_DIR")).map { logDir =>
      Files.createTempDirectory(Paths.get(logDir), s"jobserver-$encodedContextName")
    }.getOrElse(Files.createTempDirectory("jobserver"))
    logger.info("Created working directory {} for context {}", contextDir: Any, name)

    // Now create the contextConfig merged with the values we need
    val mergedContextConfig = ConfigFactory.parseMap(
      Map("is-adhoc" -> isAdHoc.toString, "context.name" -> name).asJava
    ).withFallback(contextConfig)

    // aco cluster address
    val clusterAddress = config.getString("aco.cluster.seed-node")
    var managerArgs = Seq(master, deployMode, clusterAddress, contextActorName, contextDir.toString,
      config.getString("spark.jobserver.port"))
    // Add driver cores and memory argument
    if (contextConfig.hasPath("driver-cores")) {
      managerArgs = managerArgs :+ contextConfig.getString("driver-cores")
    } else {
      managerArgs = managerArgs :+ "1"
    }
    if (contextConfig.hasPath("driver-memory")) {
      managerArgs = managerArgs :+ contextConfig.getString("driver-memory")
    } else {
      managerArgs = managerArgs :+ "0"
    }

    // Add mesos dispatcher address to arguments
    if (contextConfig.hasPath("mesos-dispatcher")) {
      managerArgs = managerArgs :+ contextConfig.getString("mesos-dispatcher")
    } else {
      managerArgs = managerArgs :+ "DEFAULT_MESOS_DISPATCHER"
    }

    if (isKubernetesMode) {
      managerArgs = managerArgs :+ encodedContextName.toLowerCase
    } else {
      managerArgs = managerArgs :+ "NULL"
    }

    //Final argument array: master, deployMode, clusterAddress, contextActorname, contextDir, httpPort,
    //  driverCores, driverMemory, mesosClusterDispatcher, Option[PROXY]
    //Notice! The spark.master and MesosClusterDispatcher is not the same, we need BOTH in mesos cluster mode
    logger.info("Ready to execute JobManager cmd with arguments : {}", managerArgs.mkString(" "))
    // extract spark.proxy.user from contextConfig, if available and pass it to manager start command
    if (contextConfig.hasPath(SparkJobUtils.SPARK_PROXY_USER_PARAM)) {
      managerArgs = managerArgs :+ contextConfig.getString(SparkJobUtils.SPARK_PROXY_USER_PARAM)
    }

    val contextLogger = LoggerFactory.getLogger("manager_start")
    val process = Process(managerStartCommand, managerArgs)
    process.run(ProcessLogger(out => contextLogger.info(out), err => contextLogger.warn(err)))

    contextInitInfos(contextActorName) = (mergedContextConfig, isAdHoc, successFunc, failureFunc)
  }

  private def addContextsFromConfig(config: Config) {
    for (contexts <- Try(config.getObject("spark.contexts"))) {
      contexts.keySet().asScala.foreach { contextName =>
        val contextConfig = config.getConfig("spark.contexts." + contextName)
          .withFallback(defaultContextConfig)
        startContext(contextName, contextConfig, false) { ref => } {
          e => logger.error("Unable to start context" + contextName, e)
        }
      }
    }

  }

  private def parameters2Map(p: String): Map[String, String] = {
    p.split("&").map {
      kv =>
        val kvArr = kv.split("=")
        if (kvArr.length == 2) {
          (kvArr(0), kvArr(1))
        } else {
          (kvArr(0), "")
        }
    }.toMap
  }
}
