package spark.jobserver

import akka.actor.{Actor, ActorIdentity, ActorRef, AddressFromURIString, Identify, RootActorPath}
import spark.jobserver.AcoSeedIdentifyActor.GetAcoSeedActorRef
import spark.jobserver.JobServer.logger


class AcoSeedIdentifyActor(acoClusterSeedNode: String) extends Actor {
  private var acoSeedActorRefOpt: Option[ActorRef] = None
  private val seeNodeAddress =
    RootActorPath(AddressFromURIString.parse(acoClusterSeedNode)) / "user" / "resource"
  private val seedNode = context.actorSelection(seeNodeAddress)
  seedNode ! Identify(seeNodeAddress)

  override def receive: Receive = {
    case ActorIdentity(memberActors, actorRefOpt) =>
      logger.info(s"Received identify response, got ActorRef of acoClusterSeedNode [$actorRefOpt]")
      acoSeedActorRefOpt = actorRefOpt
    case GetAcoSeedActorRef =>
      /*var times = 30
      while (acoSeedActorRefOpt.isEmpty && times > 0) {
        Thread.sleep(1000)
        times = times - 1
      }*/
      sender() ! acoSeedActorRefOpt
  }
}

object AcoSeedIdentifyActor {

  case object GetAcoSeedActorRef

}
