package spark.jobserver.clients.kubernetes.ssl

import java.security.cert.X509Certificate
import javax.net.ssl.X509TrustManager

class DummyTrustManager extends X509TrustManager{
  override def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}

  override def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}

  override def getAcceptedIssuers: Array[X509Certificate] = Array.empty[X509Certificate]
}
