package spark.jobserver.kubernetes.allocator

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import spark.jobserver.kubernetes.allocator.ResourceAllocator._
import spark.jobserver.kubernetes.client.K8sHttpClient

import scala.collection.mutable
import scala.util.Try

class ResourceAllocator(config: Config)(implicit system: ActorSystem) {

  private val log = LoggerFactory.getLogger(getClass)
  private val nodeRemainingResourceMap = mutable.Map.empty[String, Resource]
  private val initNodeRemainingResourceMap = mutable.Map.empty[String, Resource]
  private val podResourceMap = mutable.Map.empty[String, Seq[PodResource]]
  private val k8sClient = new K8sHttpClient(config)
  log.info(s"contextNodes: ${config.getString("kubernetes.context.nodes")}")
  private val contextNodes = config.getString("kubernetes.context.nodes").split(",")
  private val SPARK_ROLE = "spark-role"
  //private val DRIVER = "driver"
  private val EXECUTOR = "executor"
  private val DEFAULT_EXECUTOR_INSTANCES =
    Try(config.getInt("kubernetes.context.executor.instances.default")).getOrElse(2)
  private val MIN_EXECUTOR_CPU = 1
  private val MIN_EXECUTOR_MEMORY = 1024
  private val DEFAULT_MEMORY_OVERHEAD = 384

  def allocate(req: AllocationRequest): AllocationResponse = {

    val driverReqCpu = req.driverResource.cpu
    val driverReqMemory = req.driverResource.memory
    val totalExecutorReqCpu = req.totalExecutorResource.cpu
    val totalExecutorReqMemory = req.totalExecutorResource.memory
    val totalReqCpu = driverReqCpu + totalExecutorReqCpu
    val totalReqMemory = driverReqMemory + totalExecutorReqMemory +
      // driver default memoryOverhead
      DEFAULT_MEMORY_OVERHEAD +
      // executor default memoryOverhead
      DEFAULT_MEMORY_OVERHEAD * DEFAULT_EXECUTOR_INSTANCES

    synchronized {
      if (nodeRemainingResourceMap.isEmpty) {
        initNodeResource()
      }
      val (node, maxRemainingResource) =
        nodeRemainingResourceMap.maxBy {
          case (_, Resource(cpu, memory)) =>
            cpu * 1000000000L + memory
        }

      log.info(s"node $node, requestCpu = $totalReqCpu, requestMemory = $totalExecutorReqMemory, " +
        s"maxRemainingResource.cpu = ${maxRemainingResource.cpu}, " +
        s"maxRemainingResource.memory = ${maxRemainingResource.memory}")

      // here MIN_EXECUTOR_CPU and MIN_EXECUTOR_MEMORY is reserved by other service
      if (maxRemainingResource.cpu - MIN_EXECUTOR_CPU < totalReqCpu
        || maxRemainingResource.memory - MIN_EXECUTOR_MEMORY < totalReqMemory) {
        throw new Exception(s"No enough resource on max-remaining-resource node $node, " +
          s"requestCpu = $totalReqCpu, requestMemory = $totalExecutorReqMemory, " +
          s"remainingCpu = ${maxRemainingResource.cpu - MIN_EXECUTOR_CPU}, " +
          s"remainingMemory = ${maxRemainingResource.memory - MIN_EXECUTOR_MEMORY}")
      }

      val (resourcePerExecutor, instances) =
        breakUpResource(totalExecutorReqCpu.toInt, totalExecutorReqMemory)
      // Temporarily deducting resource, it's not accurate, will correct it after pod created
      val resource = Resource(totalReqCpu, totalReqMemory)
      updateNodeResource(node, resource, "-")
      AllocationResponse(req.driverResource, resourcePerExecutor, instances, node)
    }
  }

  def recycleNodeResource(contextName: String): Unit = {
    synchronized {
      val podResourceSeq = podResourceMap.getOrElse(contextName, Seq.empty[PodResource])
      val (ctxCpu, ctxMemory) = podResourceSeq.map {
        v => (v.cpu, v.memory)
      }.fold((0, 0)) {
        case (r1, r2) =>
          (r1._1 + r2._1, r1._2 + r2._2)
      }

      if (podResourceSeq.nonEmpty) {
        val node = podResourceSeq.head.nodeName
        log.info(s"Recycling context($contextName) resource")
        val ctxResource = Resource(ctxCpu, ctxMemory)
        updateNodeResource(node, ctxResource, "+")
        podResourceMap -= contextName
      }
    }
  }

  // correct node remaining resource by k8s API
  def correctNodeResource(contextName: String): Unit = {
    synchronized {
      val podResourceSeq = getContextUsedResource(contextName)
      podResourceMap += (contextName -> podResourceSeq)
      log.info(s"podResourceMap:\n$podResourceMap")
      val podList = podResourceMap.values.reduce((s1, s2) => s1 ++ s2)
      val currUsedMap = podList.groupBy(_.nodeName).map {
        case (node, seq) =>
          val (totalCpu, totalMem) = seq.map {
            v => (v.cpu, v.memory)
          }.fold((0, 0)) {
            case (r1, r2) =>
              (r1._1 + r2._1, r1._2 + r2._2)
          }
          (node, Resource(totalCpu, totalMem))
      }
      log.info(s"currUsedMap:\n$currUsedMap")
      log.info(s"nodeRemainingResourceMap:\n$nodeRemainingResourceMap")
      currUsedMap.foreach{
        case (node, resource) =>
          val remainingResource = initNodeRemainingResourceMap.getOrElse(node, Resource(0, 0)) -
            resource - Resource(MIN_EXECUTOR_CPU, MIN_EXECUTOR_MEMORY)
          nodeRemainingResourceMap += (node -> remainingResource)
          log.info(s"Finished correct node($node) resource, remaining-cpu = ${remainingResource.cpu}, " +
            s"remaining-memory = ${remainingResource.memory}")
      }
    }
  }

  private def updateNodeResource(node: String, resource: Resource, operator: String): Unit = {
    val currResource = nodeRemainingResourceMap.getOrElse(node, Resource(0, 0))
    val remainingResource = operator match {
      case "+" => currResource + resource
      case "-" => currResource - resource
    }
    nodeRemainingResourceMap += (node -> remainingResource)
    log.info(s"Update node($node) resource, " +
      s"cpu = ${resource.cpu}, memory = ${resource.memory}, " +
      s"remaining-cpu = ${remainingResource.cpu}, remaining-memory = ${remainingResource.memory}")
  }

  private def breakUpResource(totalCpu: Int, totalMemory: Int): (Resource, Int) = {
    totalCpu match {
      case MIN_EXECUTOR_CPU => (Resource(MIN_EXECUTOR_CPU, totalMemory), 1)
      case _ if totalCpu < DEFAULT_EXECUTOR_INSTANCES =>
        val memPerExecutor = totalMemory / totalCpu
        if (memPerExecutor >= MIN_EXECUTOR_MEMORY) {
          (Resource(1, memPerExecutor), totalCpu)
        } else {
          val instances = math.max(totalMemory / MIN_EXECUTOR_MEMORY, 1)
          val cpuPerExecutor = math.max(totalCpu / instances, MIN_EXECUTOR_CPU)
          (Resource(cpuPerExecutor, MIN_EXECUTOR_MEMORY), instances)
        }
      case _ =>
        val memPerExecutor = totalMemory / DEFAULT_EXECUTOR_INSTANCES
        if (memPerExecutor >= MIN_EXECUTOR_MEMORY) {
          val cpuPerExecutor = totalCpu / DEFAULT_EXECUTOR_INSTANCES
          (Resource(cpuPerExecutor, memPerExecutor), DEFAULT_EXECUTOR_INSTANCES)
        } else {
          val instances = math.max(totalMemory / MIN_EXECUTOR_MEMORY, 1)
          val cpuPerExecutor = math.max(totalCpu / instances, MIN_EXECUTOR_CPU)
          (Resource(cpuPerExecutor, MIN_EXECUTOR_MEMORY), instances)
        }
    }
  }

  private def getContextUsedResource(contextName: String): Seq[PodResource] = {
    val driverResource = k8sClient.getPodResource(contextName)
    val executorResourceSeq = k8sClient.podList(SPARK_ROLE, EXECUTOR).filter(_.owner == contextName)
    executorResourceSeq :+ driverResource
  }

  private def initNodeResource(): Unit = {
    contextNodes.foreach {
      n =>
        val (totalCpu, totalMemory) = k8sClient.nodeStatus(n)
        initNodeRemainingResourceMap += (n -> Resource(totalCpu, totalMemory))
        nodeRemainingResourceMap += (n -> Resource(totalCpu, totalMemory))
        log.info(s"Finished init node($n), cpu = $totalCpu, memory = $totalMemory")
    }
  }

}

object ResourceAllocator {

  case class AllocationRequest(driverResource: Resource,
                               totalExecutorResource: Resource)

  case class AllocationResponse(driverResource: Resource,
                                executorResource: Resource,
                                executorInstances: Int,
                                nodeName: String)

  // memory unit is MB
  case class Resource(cpu: Int, memory: Int) {
    def -(that: Resource): Resource = {
      val newCpu = cpu - that.cpu
      val newMem = memory - that.memory
      Resource(newCpu, newMem)
    }

    def +(that: Resource): Resource = {
      val newCpu = cpu + that.cpu
      val newMem = memory + that.memory
      Resource(newCpu, newMem)
    }
  }

  case class PodResource(podName: String, nodeName: String, cpu: Int, memory: Int, owner: String = "")

}
