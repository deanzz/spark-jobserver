package spark.jobserver.clients.kubernetes

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import spark.jobserver.clients.kubernetes.json.JsonUtils
import spray.client.pipelining._
import spray.http.{HttpRequest, HttpResponse}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

class K8sHttpClient(config: Config)(implicit system: ActorSystem) {

  private val log = LoggerFactory.getLogger(getClass)
  private val host = config.getString("kubernetes.api.host")
  private val defaultNamespace = config.getString("kubernetes.api.namespace")
  private val requestTimeout = config.getInt("kubernetes.api.timeout.seconds")

  def pipeline: HttpRequest => Future[HttpResponse] = sendReceive

  def podStatus(name: String, namespace: String = defaultNamespace): String = {
    val url = s"$host/api/v1/namespaces/$namespace/pods/$name/status"
    val future = pipeline(Get(url)).map(res => parsePodStatusResp(res.entity.asString))
    Try(Await.result(future, requestTimeout seconds)) match {
      case Success(res) => res
      case Failure(e) =>
        log.error(s"podStatus got error, ${e.getMessage}", e)
        throw e
    }
  }

  def podLog(name: String, namespace: String = defaultNamespace,
             previous: Boolean = true, tailLines: Int = 200): String = {
    val url = s"$host/api/v1/namespaces/$namespace/pods/$name/log?previous=$previous&tailLines=$tailLines"
    val future = pipeline(Get(url)).map(res => res.entity.asString)
    Try(Await.result(future, requestTimeout seconds)) match {
      case Success(res) => res
      case Failure(e) =>
        log.error(s"podPreviousLog got error, ${e.getMessage}", e)
        throw e
    }
  }

  private def parsePodStatusResp(resp: String) = {
    val map = JsonUtils.mapFromJson(resp)
    val status = map("status")
    if (status.asInstanceOf[String] == "Failure") {
      map("reason").asInstanceOf[String]
    } else {
      status.asInstanceOf[Map[String, Any]]("phase").asInstanceOf[String]
    }

  }
}
