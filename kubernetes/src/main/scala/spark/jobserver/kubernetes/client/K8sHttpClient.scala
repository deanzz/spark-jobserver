package spark.jobserver.kubernetes.client

import java.net.URLEncoder
import java.security.SecureRandom
import java.util.concurrent.TimeUnit
import javax.net.ssl.{SSLContext, TrustManager}

import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import spark.jobserver.kubernetes.allocator.ResourceAllocator.PodResource
import spark.jobserver.kubernetes.client.json.JsonUtils
import spark.jobserver.kubernetes.client.ssl.DummyTrustManager
import spray.can.Http
import spray.can.Http.{HostConnectorInfo, HostConnectorSetup}
import spray.client.pipelining._
import spray.http.{HttpRequest, HttpResponse}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class K8sHttpClient(config: Config)(implicit system: ActorSystem) {

  private val log = LoggerFactory.getLogger(getClass)
  private val host = config.getString("kubernetes.api.host")
  private val port = config.getInt("kubernetes.api.port")
  private val defaultNamespace = config.getString("kubernetes.api.namespace")
  private val requestTimeout = config.getInt("kubernetes.api.timeout.seconds")

  implicit def sslContext: SSLContext = {
    val context = SSLContext.getInstance("TLS")
    context.init(null, Array[TrustManager](new DummyTrustManager), new SecureRandom())
    context
  }

  private implicit val timeout = akka.util.Timeout(requestTimeout, TimeUnit.SECONDS)

  def pipeline: HttpRequest => Future[HttpResponse] = {
    val connection = {
      Await.result((IO(Http) ? HostConnectorSetup(host, port = port, sslEncryption = true)).map {
        case HostConnectorInfo(hostConnector, _) => hostConnector
      }, requestTimeout seconds)
    }
    sendReceive(connection)
  }

  def podStatus(name: String, namespace: String = defaultNamespace): String = {
    val uri = s"/api/v1/namespaces/$namespace/pods/$name/status"
    log.info(s"podStatus.uri = $uri")
    val future = pipeline(Get(uri)).map(res => res.entity.asString)
    Try(Await.result(future, requestTimeout seconds)) match {
      case Success(res) => res
      case Failure(e) =>
        log.error(s"podStatus got error, ${e.getMessage}", e)
        throw e
    }
  }

  def getPodStatus(name: String, namespace: String = defaultNamespace): String = {
    parsePodStatusResp(podStatus(name, namespace))
  }

  def getPodResource(name: String, namespace: String = defaultNamespace): PodResource = {
    parsePodResourceResp(podStatus(name, namespace))
  }

  def podLog(name: String, namespace: String = defaultNamespace,
             previous: Boolean = false, tailLines: Int = 200): String = {
    val uri =
      s"/api/v1/namespaces/$namespace/pods/$name/log?previous=$previous&tailLines=$tailLines"
    log.info(s"podLog.uri = $uri")
    val future = pipeline(Get(uri)).map(res => res.entity.asString)
    Try(Await.result(future, requestTimeout seconds)) match {
      case Success(res) => res
      case Failure(e) =>
        log.error(s"podPreviousLog got error, ${e.getMessage}", e)
        throw e
    }
  }

  def deletePod(name: String, namespace: String = defaultNamespace): Boolean = {
    val uri = s"/api/v1/namespaces/$namespace/pods/$name?gracePeriodSeconds=0"
    log.info(s"deletePod.uri = $uri")
    val future = pipeline(Delete(uri)).map(res => parsePodStatusResp(res.entity.asString))
    Try(Await.result(future, requestTimeout seconds)) match {
      case Success(res) => true
      case Failure(e) =>
        log.error(s"deletePod got error, ${e.getMessage}", e)
        throw e
    }
  }

  def nodeStatus(name: String): (Int, Int) = {
    val uri = s"/api/v1/nodes/$name/status"
    log.info(s"nodeStatus.uri = $uri")
    val future = pipeline(Get(uri)).map(res => parseNodeStatusResp(res.entity.asString))
    Try(Await.result(future, requestTimeout seconds)) match {
      case Success(res) => res
      case Failure(e) =>
        log.error(s"nodeStatus got error, ${e.getMessage}", e)
        throw e
    }
  }

  def podList(labelKey: String, labelVal: String,
              namespace: String = defaultNamespace): Seq[PodResource] = {
    val encodeLabel = URLEncoder.encode(s"$labelKey=$labelVal", "UTF-8")
    val uri = s"/api/v1/namespaces/$namespace/pods?labelSelector=$encodeLabel"
    log.info(s"podList.uri = $uri")
    val future = pipeline(Get(uri)).map(res => parsePodListResp(res.entity.asString))
    Try(Await.result(future, requestTimeout seconds)) match {
      case Success(res) => res
      case Failure(e) =>
        log.error(s"podStatus got error, ${e.getMessage}", e)
        throw e
    }
  }

  private def parsePodListResp(resp: String): Seq[PodResource] = {
    log.info(s"resp:\n$resp")
    val map = JsonUtils.mapFromJson(resp)
    val items = map("items").asInstanceOf[List[Map[String, Any]]]
    items.map {
      m =>
        val metadata = m("metadata").asInstanceOf[Map[String, Any]]
        val podName = metadata("name").asInstanceOf[String]
        val ownerReferences = metadata.getOrElse("ownerReferences", List.empty[Map[String, Any]])
          .asInstanceOf[List[Map[String, Any]]]
        val owner =
          if (ownerReferences.isEmpty) {
            ""
          } else {
            ownerReferences.head.getOrElse("name", "").asInstanceOf[String]
          }

        val spec = m("spec").asInstanceOf[Map[String, Any]]
        val nodeName = spec("nodeName").asInstanceOf[String]
        val containers = spec("containers").asInstanceOf[List[Map[String, Any]]]
        val resources = containers.head("resources").asInstanceOf[Map[String, Any]]
        val limits = resources("limits").asInstanceOf[Map[String, Any]]
        val cpuStr = limits("cpu").asInstanceOf[String]
        val cpuNum = cpuStr.filter(c => c >= '0' && c <= '9').toInt
        val cpu =
          if (cpuStr.endsWith("m")) {
            val num = cpuNum / 1000
            if (num < 1) 1 else num
          } else {
            cpuNum
          }
        // MB
        val memory = limits("memory").asInstanceOf[String].filter(c => c >= '0' && c <= '9').toInt
        PodResource(podName, nodeName, cpu, memory, owner)
    }
  }

  private def parsePodResourceResp(resp: String): PodResource = {
    log.info(s"resp:\n$resp")
    val map = JsonUtils.mapFromJson(resp)
    val metadata = map("metadata").asInstanceOf[Map[String, Any]]
    val podName = metadata("name").asInstanceOf[String]
    val spec = map("spec").asInstanceOf[Map[String, Any]]
    val nodeName = spec("nodeName").asInstanceOf[String]
    val containers = spec("containers").asInstanceOf[List[Map[String, Any]]]
    val resources = containers.head("resources").asInstanceOf[Map[String, Any]]
    val limits = resources("limits").asInstanceOf[Map[String, Any]]
    val cpuStr = limits("cpu").asInstanceOf[String]
    val cpuNum = cpuStr.filter(c => c >= '0' && c <= '9').toInt
    val cpu =
      if (cpuStr.endsWith("m")) {
        val num = cpuNum / 1000
        if (num < 1) 1 else num
      } else {
        cpuNum
      }
    // MB
    val memory = limits("memory").asInstanceOf[String].filter(c => c >= '0' && c <= '9').toInt
    PodResource(podName, nodeName, cpu, memory)
  }

  private def parsePodStatusResp(resp: String) = {
    log.info(s"resp:\n$resp")
    val map = JsonUtils.mapFromJson(resp)
    val status = map("status")
    if (Try(status.asInstanceOf[String]).getOrElse("") == "Failure") {
      map("reason").asInstanceOf[String]
    } else {
      status.asInstanceOf[Map[String, Any]]("phase").asInstanceOf[String]
    }
  }

  private def parseNodeStatusResp(resp: String): (Int, Int) = {
    log.info(s"resp:\n$resp")
    val map = JsonUtils.mapFromJson(resp)
    if (resp.contains("\"status\"") && resp.contains("\"allocatable\"")) {
      val status = map("status")
      val allocatableMap = status.asInstanceOf[Map[String, Any]]("allocatable").asInstanceOf[Map[String, Any]]

      val cpuStr = allocatableMap("cpu").asInstanceOf[String]
      val cpuNum = cpuStr.filter(c => c >= '0' && c <= '9').toInt
      val cpu =
        if (cpuStr.endsWith("m")) {
          val num = cpuNum / 1000
          if (num < 1) 1 else num
        } else {
          cpuNum
        }

      // kB -> MB
      val memory = (allocatableMap("memory").toString.filter(c => c >= '0' && c <= '9').toLong / 1024).toInt
      (cpu, memory)
    } else {
      val reason = map.getOrElse("reason", s"Error json format,\n$resp").asInstanceOf[String]
      throw new Exception(reason)
    }
  }
}

