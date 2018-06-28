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
import spark.jobserver.kubernetes.client.K8sHttpClient.{IngressRule, Service}
import spark.jobserver.kubernetes.client.json.JsonUtils
import spark.jobserver.kubernetes.client.ssl.DummyTrustManager
import spray.can.Http
import spray.can.Http.{HostConnectorInfo, HostConnectorSetup}
import spray.client.pipelining._
import spray.http._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import spray.http.HttpHeaders._

class K8sHttpClient(config: Config)(implicit system: ActorSystem) {

  private val log = LoggerFactory.getLogger(getClass)
  private val host = config.getString("kubernetes.api.host")
  private val port = config.getInt("kubernetes.api.port")
  private val defaultNamespace = config.getString("kubernetes.api.namespace")
  private val requestTimeout = config.getInt("kubernetes.api.timeout.seconds")
  private val sparkUIPort = Try(config.getInt("kubernetes.sparkui.port")).getOrElse(4040)
  private val sparkUIPortName = Try(config.getString("kubernetes.sparkui.portname"))
    .getOrElse("sparkuiport")
  private val sparkUIIngressName = Try(config.getString("kubernetes.sparkui.ingress.name"))
    .getOrElse("ingress-elemental-driver")
  private val sparkUIHostSuffix = Try(config.getString("kubernetes.sparkui.host.suffix"))
    .getOrElse(".driver.kmtongji.com")

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

  def serviceList(namespace: String = defaultNamespace): Seq[Service] = {
    val uri = s"/api/v1/namespaces/$namespace/services"
    log.info(s"serviceList.uri = $uri")
    val future = pipeline(Get(uri)).map(res => parseServiceListResp(res.entity.asString))
    Try(Await.result(future, requestTimeout seconds)) match {
      case Success(res) => res
      case Failure(e) =>
        log.error(s"serviceList got error, ${e.getMessage}", e)
        throw e
    }
  }

  def getServiceByPod(podName: String,
                      namespace: String = defaultNamespace): Option[Service] = {
    val list = serviceList(namespace)
    list.find(_.owner == podName)
  }

  def openServicePort(serviceName: String, port: Int = sparkUIPort, portName: String = sparkUIPortName,
                      namespace: String = defaultNamespace): Boolean = {
    val uri = s"/api/v1/namespaces/$namespace/services/$serviceName"
    val reqBody =
      s"""
         |[{
         |  "op": "add",
         |  "path": "/spec/ports/-",
         |  "value": {
         |    "name": "$portName",
         |    "protocol": "TCP",
         |    "port": $port,
         |    "targetPort": $port
         |  }
         |}]
      """.stripMargin.replace("\n", "").replace(" ", "")
    log.info(s"openServicePort.uri = $uri")
    val future = pipeline(Patch(uri)
      .withEntity(
        HttpEntity(ContentType(MediaTypes.`application/json-patch+json`, HttpCharsets.`UTF-8`), reqBody)))
      .map(res => res)
    Try(Await.result(future, requestTimeout seconds)) match {
      case Success(res) => log.info(s"openServicePort resp:\n$res")
        res.status == StatusCodes.OK
      case Failure(e) =>
        log.error(s"openServicePort got error, ${e.getMessage}", e)
        throw e
    }
  }

  def ingressRules(name: String = sparkUIIngressName,
                   namespace: String = defaultNamespace): Seq[IngressRule] = {
    val uri = s"/apis/extensions/v1beta1/namespaces/$namespace/ingresses/$name"
    log.info(s"ingressRules.uri = $uri")
    val future = pipeline(Get(uri)).map(res => parseIngressRulesResp(res.entity.asString))
    Try(Await.result(future, requestTimeout seconds)) match {
      case Success(res) => res
      case Failure(e) =>
        log.error(s"serviceList got error, ${e.getMessage}", e)
        throw e
    }
  }

  def newIngressRule(podName: String, serviceName: String, port: Int = sparkUIPort,
                     ingressName: String = sparkUIIngressName,
                     namespace: String = defaultNamespace): (Boolean, String) = {
    val uri = s"/apis/extensions/v1beta1/namespaces/$namespace/ingresses/$ingressName"
    log.info(s"newIngressRule.uri = $uri")
    val rules = ingressRules(ingressName, namespace)
    val reqBody = rules.find(_.host.startsWith(podName)) match {
      case Some(rule) =>
        // update rule
        s"""
           |[{
           |  "op": "replace",
           |  "path": "/spec/rules/${rule.index}/http/paths/0/backend/serviceName",
           |  "value": "$serviceName"
           |}]
           """.stripMargin.replace("\n", "").replace(" ", "")
      case None =>
        // add new rule
        s"""
           |[{
           |  "op": "add",
           |  "path": "/spec/rules/-",
           |  "value": {
           |    "host": "$podName$sparkUIHostSuffix",
           |    "http": {
           |      "paths": [{
           |        "backend": {
           |        "serviceName": "$serviceName",
           |        "servicePort": $port
           |        }
           |      }]
           |    }
           |  }
           |}]
           """.stripMargin.replace("\n", "").replace(" ", "")
    }

    val future = pipeline(Patch(uri)
      .withEntity(
        HttpEntity(ContentType(MediaTypes.`application/json-patch+json`, HttpCharsets.`UTF-8`), reqBody)))
      .map(res => res)
    Try(Await.result(future, requestTimeout seconds)) match {
      case Success(res) => log.info(s"newIngressRule resp:\n$res")
        (res.status == StatusCodes.OK, s"$podName$sparkUIHostSuffix")
      case Failure(e) =>
        log.error(s"newIngressRule got error, ${e.getMessage}", e)
        throw e
    }
  }

  def removeIngressRule(podName: String,
                        ingressName: String = sparkUIIngressName,
                        namespace: String = defaultNamespace): Boolean = {
    val uri = s"/apis/extensions/v1beta1/namespaces/$namespace/ingresses/$ingressName"
    log.info(s"removeIngressRule.uri = $uri")
    val rules = ingressRules(ingressName, namespace)
    rules.find(_.host.startsWith(podName)) match {
      case Some(rule) =>
        val reqBody =
          s"""
             |[{
             |  "op": "remove",
             |  "path": "/spec/rules/${rule.index}"
             |}]
           """.stripMargin.replace("\n", "").replace(" ", "")
        val future = pipeline(Patch(uri)
          .withEntity(
            HttpEntity(ContentType(MediaTypes.`application/json-patch+json`, HttpCharsets.`UTF-8`), reqBody)))
          .map(res => res)
        Try(Await.result(future, requestTimeout seconds)) match {
          case Success(res) => log.info(s"removeIngressRule resp:\n$res")
            res.status == StatusCodes.OK
          case Failure(e) =>
            log.error(s"removeIngressRule got error, ${e.getMessage}", e)
            throw e
        }
      case None => false
    }
  }


  private def parsePodListResp(resp: String): Seq[PodResource] = {
    log.debug(s"resp:\n$resp")
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
    log.debug(s"resp:\n$resp")
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
    log.debug(s"resp:\n$resp")
    val map = JsonUtils.mapFromJson(resp)
    val status = map("status")
    if (Try(status.asInstanceOf[String]).getOrElse("") == "Failure") {
      map("reason").asInstanceOf[String]
    } else {
      status.asInstanceOf[Map[String, Any]]("phase").asInstanceOf[String]
    }
  }

  private def parseNodeStatusResp(resp: String): (Int, Int) = {
    log.debug(s"resp:\n$resp")
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

  private def parseServiceListResp(resp: String): Seq[Service] = {
    log.debug(s"resp:\n$resp")
    val map = JsonUtils.mapFromJson(resp)
    val items = map("items").asInstanceOf[List[Map[String, Any]]]
    items.map {
      m =>
        val metadata = m("metadata").asInstanceOf[Map[String, Any]]
        val name = metadata("name").asInstanceOf[String]
        val ownerReferences = metadata.getOrElse("ownerReferences", List.empty[Map[String, Any]])
          .asInstanceOf[List[Map[String, Any]]]
        val owner =
          if (ownerReferences.isEmpty) {
            ""
          } else {
            ownerReferences.head.getOrElse("name", "").asInstanceOf[String]
          }

        Service(name, owner)
    }
  }

  private def parseIngressRulesResp(resp: String): Seq[IngressRule] = {
    log.debug(s"resp:\n$resp")
    val map = JsonUtils.mapFromJson(resp)
    val spec = map("spec").asInstanceOf[Map[String, Any]]
    val rules = spec("rules").asInstanceOf[List[Map[String, Any]]]
    rules.zipWithIndex.map {
      case (m, idx) =>
        val host = m("host").asInstanceOf[String]
        val http = m("http").asInstanceOf[Map[String, Any]]
        val paths = http("paths").asInstanceOf[List[Map[String, Any]]]
        val path = paths.head
        val backend = path("backend").asInstanceOf[Map[String, Any]]
        val serviceName = backend("serviceName").asInstanceOf[String]
        val servicePort = backend("servicePort").asInstanceOf[Int]
        IngressRule(idx, host, serviceName, servicePort)
    }
  }

}

object K8sHttpClient {

  case class Service(name: String, owner: String)

  case class IngressRule(index: Int, host: String, serviceName: String, servicePort: Int)

}

