package spark.jobserver.clients.kubernetes

import java.security.SecureRandom
import java.util.concurrent.TimeUnit
import javax.net.ssl.{SSLContext, TrustManager}

import akka.actor.ActorSystem
import akka.io.IO
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import spark.jobserver.clients.kubernetes.json.JsonUtils
import spark.jobserver.clients.kubernetes.ssl.DummyTrustManager
import spray.can.Http
import spray.can.Http.{HostConnectorInfo, HostConnectorSetup}
import spray.client.pipelining._
import spray.http.{HttpRequest, HttpResponse}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.ask

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
  private val connection = {
    Await.result((IO(Http) ? HostConnectorSetup(host, port = port, sslEncryption = true)).map {
      case HostConnectorInfo(hostConnector, _) => hostConnector }, requestTimeout seconds)
  }

  def pipeline: HttpRequest => Future[HttpResponse] = {
    sendReceive(connection)
  }

  /*def pipeline: HttpRequest => Future[HttpResponse] = {
    val connection = {
      Await.result((IO(Http) ? HostConnectorSetup(host, port = port, sslEncryption = true)).map {
        case HostConnectorInfo(hostConnector, _) => hostConnector }, requestTimeout seconds)
    }
    sendReceive(connection)
  }*/

  def podStatus(name: String, namespace: String = defaultNamespace): String = {
    val uri = s"/api/v1/namespaces/$namespace/pods/$name/status"
    log.info(s"podStatus.uri = $uri")
    val future = pipeline(Get(uri)).map(res => parsePodStatusResp(res.entity.asString))
    Try(Await.result(future, requestTimeout seconds)) match {
      case Success(res) => res
      case Failure(e) =>
        log.error(s"podStatus got error, ${e.getMessage}", e)
        throw e
    }
  }

  def podLog(name: String, namespace: String = defaultNamespace,
             previous: Boolean = true, tailLines: Int = 200): String = {
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

  private def parsePodStatusResp(resp: String) = {
    log.info(s"resp:\n$resp")
    val map = JsonUtils.mapFromJson(resp)
    val status = map("status")
    if (status.asInstanceOf[String] == "Failure") {
      map("reason").asInstanceOf[String]
    } else {
      status.asInstanceOf[Map[String, Any]]("phase").asInstanceOf[String]
    }

  }
}
