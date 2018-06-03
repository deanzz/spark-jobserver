package spark.jobserver

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import aco.jobserver.common.JobServerMessage._
import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberUp, UnreachableMember}
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
import spark.jobserver.AkkaClusterSupervisorActor.{
  BatchCreatingRefreshContext, BatchCreatingUpdateContext, CreatingContextRequest
}
import spark.jobserver.JobManagerActor.{GetSparkWebUIUrl, NoSparkWebUI, SparkContextDead, SparkWebUIUrl}
import spark.jobserver.io.JobDAOActor.CleanContextJobInfos
import spark.jobserver.kubernetes.allocator.ResourceAllocator
import spark.jobserver.kubernetes.allocator.ResourceAllocator.{AllocationRequest, Resource}
import spark.jobserver.kubernetes.client.K8sHttpClient

import scala.collection.mutable.ListBuffer

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
  private val resourceAllocator = new ResourceAllocator(config)

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

  private val creatingRefreshContextRequests = ListBuffer.empty[CreatingContextRequest]
  private val creatingUpdateContextRequests = ListBuffer.empty[CreatingContextRequest]
  private val BATCH_CREATING_CONTEXT_WAITING_SECOND = 3

  logger.info("AkkaClusterSupervisor initialized on {}", selfAddress)


  override def preStart(): Unit = {
    //cluster.join(selfAddress)
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
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
    //todo 当发觉scheduler重新启动后，重新AddJobServer到scheduler
    /*if (member.hasRole("scheduler")) {
      Thread.sleep(10000)
      /*acoMonitor = Some(context.system.actorOf(ClusterSingletonProxy.props(
        singletonManagerPath = "/user/jobserver-monitor",
        settings = ClusterSingletonProxySettings(context.system).withRole("scheduler")),
        name = "jobserver-monitor-proxy"))*/
      acoMonitor.foreach(m => m ! AddJobServerWrapper(self.path.toString))
    }*/

    case UnreachableMember(member) =>
      logger.error(s"UnreachableMember: roles ${member.roles}, address ${member.address}")

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
      val paramMap = parameters2Map(parameters)
      val contextConfig = ConfigFactory.parseMap(paramMap.asJava)

      if (enableK8sCheck) {
        val driverCpu = Try(contextConfig.getInt("driver-cores")).getOrElse(1)
        val driverMemory = Try(contextConfig.getString("driver-memory"))
          .getOrElse("1024m").filter(c => c >= '0' && c <= '9').toInt
        val totalExecutorCpu = Try(contextConfig.getInt("num-cpu-cores")).getOrElse(1)
        val totalExecutorMemory = Try(contextConfig.getString("memory-per-node"))
          .getOrElse("1024m").filter(c => c >= '0' && c <= '9').toInt

        val creatingContextRequest = CreatingContextRequest(
          name, driverCpu, driverMemory, totalExecutorCpu, totalExecutorMemory,
          contextConfig, requestWorker, operatorId, operation)
        operation match {
          case "Refresh" =>
            creatingRefreshContextRequests += creatingContextRequest
            context.system.scheduler.scheduleOnce(BATCH_CREATING_CONTEXT_WAITING_SECOND seconds,
              self, BatchCreatingRefreshContext)
          case "Update" =>
            creatingUpdateContextRequests += creatingContextRequest
            context.system.scheduler.scheduleOnce(BATCH_CREATING_CONTEXT_WAITING_SECOND seconds,
              self, BatchCreatingUpdateContext)
          case _ =>
            createOnK8s(contextConfig, driverCpu, driverMemory, totalExecutorCpu,
              totalExecutorMemory, name, requestWorker, operatorId, operation)
        }
      } else {
        self ! AddContext(name, contextConfig, Some(requestWorker), Some(operatorId), Some(operation))
      }

    case BatchCreatingRefreshContext =>
      if (creatingRefreshContextRequests.nonEmpty) {
        // creating context in descending order of the number of cpu cores
        val list = creatingRefreshContextRequests.sortBy {
          c =>
            (c.driverCpu + c.totalExecutorCpu) * 100000000 + (c.driverMemory + c.totalExecutorMemory)
        }.reverse
        logger.info(s"Start BatchCreatingRefreshContext, creatingRefreshContextRequests:" +
          s"\n${list.map(_.toString).mkString("\n")}")
        list.foreach {
          ctx =>
            createOnK8s(ctx.config, ctx.driverCpu, ctx.driverMemory, ctx.totalExecutorCpu,
              ctx.totalExecutorMemory, ctx.name, ctx.requestWorker, ctx.operatorId, ctx.operation)
        }
        creatingRefreshContextRequests.clear()
      }

    case BatchCreatingUpdateContext =>
      if (creatingUpdateContextRequests.nonEmpty) {
        // creating context in descending order of the number of cpu cores
        val list = creatingUpdateContextRequests.sortBy {
          c =>
            (c.driverCpu + c.totalExecutorCpu) * 100000000 + (c.driverMemory + c.totalExecutorMemory)
        }.reverse
        logger.info(s"Start BatchCreatingUpdateContext, creatingUpdateContextRequests:" +
          s"\n${list.map(_.toString).mkString("\n")}")
        list.foreach {
          ctx =>
            createOnK8s(ctx.config, ctx.driverCpu, ctx.driverMemory, ctx.totalExecutorCpu,
              ctx.totalExecutorMemory, ctx.name, ctx.requestWorker, ctx.operatorId, ctx.operation)
        }
        creatingUpdateContextRequests.clear()
      }

    case AddContext(name, contextConfig, workerActor, operatorId, operation) =>
      // using lower-case context name ONLY under k8s API
      val lowerName = name.toLowerCase
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
          acoMonitor.foreach(m => m ! succeed)
        }
      } else {
        startContext(name, mergedConfig, isAdHoc = false) { ref =>
          originator ! ContextInitialized
          if (workerActor.isDefined) {
            Try {
              // reduce node resource
              resourceAllocator.correctNodeResource(lowerName)
            } match {
              case Success(_) =>
              case Failure(e) =>
                val errMsg = s"Reduce node resource failed, " +
                  s"${e.getMessage}\n${e.getStackTrace.map(t => t.toString).mkString("\n")}"
                logger.error(s"Reduce node resource failed, ${e.getMessage}", e)
                val failed = AddContextFailedWrapper(name, errMsg, operatorId.get, operation.get)
                workerActor.get ! failed
                acoMonitor.foreach(m => m ! failed)
            }

            val succeed = AddContextSucceedWrapper(name, ref.path.toString,
              operatorId.get, "SUCCESS", operation.get)
            workerActor.get ! succeed
            acoMonitor.foreach(m => m ! succeed)
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
        .map(SparkJobUtils.userNamePrefix).getOrElse("")
      var contextName = ""
      do {
        contextName = userNamePrefix + java.util.UUID.randomUUID().toString.take(8) + "-" + classPath
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
          if (enableK8sCheck) {
            val lowerName = name.toLowerCase
            // increase node resource
            resourceAllocator.recycleNodeResource(lowerName)
          }
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
        if (enableK8sCheck) {
          // increase node resource
          val lowerName = name.toLowerCase
          resourceAllocator.recycleNodeResource(lowerName)
          /*Try(k8sClient.podLog(lowerName)) match {
            case Success(log) =>
              logger.error(s"error log from k8s:\n$log")
              acoMonitor.foreach(m => m ! ContextTerminated(name, log))
            case Failure(e) =>
              val errMsg = e match {
                case _: TimeoutException => "Connect kubernetes API timeout!!"
                case _ => s"${e.getMessage}\n${e.getStackTrace.map(_.toString).mkString("\n")}"
              }
              acoMonitor.foreach(m => m ! ContextTerminated(name, errMsg))
          }*/
          val errMsg = s"Context($name) terminated! it will return error log on the future version"
          acoMonitor.foreach(m => m ! ContextTerminated(name, errMsg))
        } else {
          acoMonitor.foreach(m => m ! ContextTerminated(name,
            "No error log when kubernetes.check.enable is false"))
        }
      }
      cluster.down(actorRef.path.address)
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
    //val encodedContextName = java.net.URLEncoder.encode(name, "UTF-8")
    val contextDir = Option(System.getProperty("LOG_DIR")).map { logDir =>
      Files.createTempDirectory(Paths.get(logDir), s"jobserver-$name")
    }.getOrElse(Files.createTempDirectory("jobserver"))
    logger.info("Created working directory {} for context {}", contextDir: Any, name)

    // Now create the contextConfig merged with the values we need
    val mergedContextConfig = ConfigFactory.parseMap(
      Map("is-adhoc" -> isAdHoc.toString, "context.name" -> name).asJava
    ).withFallback(contextConfig)

    // aco cluster address
    val clusterAddress = config.getString("aco.cluster.seed-node")
    var managerArgs = Seq(master, deployMode, clusterAddress, contextActorName, contextDir.toString,
      config.getString("spark.jobserver.port"), selfAddress.host.getOrElse(""))
    // Add driver cores and memory argument
    if (contextConfig.hasPath("driver-cores")) {
      managerArgs = managerArgs :+ contextConfig.getString("driver-cores")
    } else {
      managerArgs = managerArgs :+ "NULL"
    }
    if (contextConfig.hasPath("driver-memory")) {
      managerArgs = managerArgs :+ contextConfig.getString("driver-memory")
    } else {
      managerArgs = managerArgs :+ "NULL"
    }

    // Add mesos dispatcher address to arguments
    if (contextConfig.hasPath("mesos-dispatcher")) {
      managerArgs = managerArgs :+ contextConfig.getString("mesos-dispatcher")
    } else {
      managerArgs = managerArgs :+ "DEFAULT_MESOS_DISPATCHER"
    }

    if (isKubernetesMode) {
      // using lower-case context name ONLY under k8s API
      managerArgs = managerArgs :+ name.toLowerCase
    } else {
      managerArgs = managerArgs :+ "NULL"
    }

    if (isKubernetesMode) {
      managerArgs = managerArgs :+ contextConfig.getString("node-selector")
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

  private def k8sConfig(contextConfig: Config,
                        driverCpu: Int, driverMemory: Int,
                        totalExecutorCpu: Int, totalExecutorMemory: Int): Config = {
    val allocationRequest = AllocationRequest(Resource(driverCpu, driverMemory),
      Resource(totalExecutorCpu, totalExecutorMemory))
    val allocationResponse = resourceAllocator.allocate(allocationRequest)
    logger.info(
      s"""
              num-cpu-cores -> ${allocationResponse.executorResource.cpu}
              memory-per-node -> ${allocationResponse.executorResource.memory}m
              executor-instances -> ${allocationResponse.executorInstances.toString}
              node-selector -> ${allocationResponse.nodeName}""")
    // write new config
    ConfigFactory.parseMap(
      Map(
        "num-cpu-cores" -> allocationResponse.executorResource.cpu.toString,
        "memory-per-node" -> s"${allocationResponse.executorResource.memory}m",
        "executor-instances" -> allocationResponse.executorInstances.toString,
        "node-selector" -> allocationResponse.nodeName).asJava
    ).withFallback(contextConfig)
  }

  private def createOnK8s(contextConfig: Config, driverCpu: Int, driverMemory: Int,
                          totalExecutorCpu: Int, totalExecutorMemory: Int, name: String,
                          requestWorker: ActorRef, operatorId: String, operation: String): Unit = {
    // using lower-case context name ONLY under k8s API
    val lowerName = name.toLowerCase
    Try(k8sClient.getPodStatus(lowerName)) match {
      case Success(res) =>
        Try {
          val mergedContextConfig = k8sConfig(
            contextConfig, driverCpu, driverMemory, totalExecutorCpu, totalExecutorMemory)
          logger.info(s"The status of context [$lowerName] in kubernetes is $res")
          res match {
            case "NotFound" =>
              self ! AddContext(name, mergedContextConfig,
                Some(requestWorker), Some(operatorId), Some(operation))
            case "Running" =>
              contexts.get(name) match {
                case Some(ctx) =>
                  val succeed = AddContextSucceedWrapper(name, ctx._1.path.toString,
                    operatorId, s"context $name exists", operation)
                  requestWorker ! succeed
                  acoMonitor.foreach(m => m ! succeed)
                case _ =>
                  logger.warn("Inconsistent jobserver and kubernetes status, " +
                    s"recreate the context: $name")
                  k8sClient.deletePod(lowerName)
                  self ! AddContext(name, mergedContextConfig,
                    Some(requestWorker), Some(operatorId), Some(operation))
              }
            case _ =>
              //"Failed" | "Unknown" | "Pending" | "Succeeded"
              k8sClient.deletePod(lowerName)
              self ! AddContext(name, mergedContextConfig,
                Some(requestWorker), Some(operatorId), Some(operation))
          }
        } match {
          case Success(_) =>
          case Failure(e) =>
            logger.error(s"Resource allocation error, ${e.getMessage}", e)
            val err = s"${e.getMessage}\n${e.getStackTrace.map(_.toString).mkString("\n")}"
            val failed = AddContextFailedWrapper(
              name, s"Resource allocation error, $err", operatorId, operation)
            requestWorker ! failed
            acoMonitor.foreach(m => m ! failed)
        }
      case Failure(e) =>
        logger.error(s"AddContextWrapper error, ${e.getMessage}", e)
        e match {
          case _: TimeoutException =>
            val failed = AddContextFailedWrapper(name,
              s"Kubernetes error, Connect kubernetes API timeout!!",
              operatorId, operation)
            requestWorker ! failed
            acoMonitor.foreach(m => m ! failed)
          case _ =>
            val err = s"${e.getMessage}\n${e.getStackTrace.map(_.toString).mkString("\n")}"
            val failed = AddContextFailedWrapper(
              name, s"Kubernetes error, $err", operatorId, operation)
            requestWorker ! failed
            acoMonitor.foreach(m => m ! failed)
        }
    }
  }
}

object AkkaClusterSupervisorActor {

  case class CreatingContextRequest(name: String, driverCpu: Int, driverMemory: Int,
                                    totalExecutorCpu: Int, totalExecutorMemory: Int, config: Config,
                                    requestWorker: ActorRef, operatorId: String, operation: String) {
    override def toString: String = {
      s"CreatingContextRequest($name, $driverCpu, $totalExecutorCpu, $driverMemory, $totalExecutorMemory)"
    }
  }

  case object BatchCreatingRefreshContext

  case object BatchCreatingUpdateContext

}
