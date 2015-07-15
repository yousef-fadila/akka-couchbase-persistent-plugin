package couchbase.perf;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.persistence.Recover;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by dannyt on 10/13/2014.
 */
public class DispatcherActor extends UntypedActor {
    private static final Logger log = LoggerFactory.getLogger(DispatcherActor.class);
    Map<String,ActorRef> actors;
    private final int expaectedMessages;
    @Override
    public void onReceive(Object msg) throws Exception {

            if(msg instanceof Recover){

                for(String actorId:actors.keySet()){

                    ActorRef statefulActor = context().system().actorOf(Props.create(StatefulActor.class,actorId,expaectedMessages));


                    actors.put(actorId, statefulActor);
                    statefulActor.tell(Recover.create(), null);
                }
            }else if("recovery".equals(msg)) {
                log.info("received recovery duration");
            }else{

                for (ActorRef actor : actors.values()) {
                    actor.forward(msg, context());


                }
            }

    }


    public DispatcherActor(int numberOfStatefulActors,int expaectedMessages){
        this.expaectedMessages = expaectedMessages;
        actors = new HashMap<>();
        for(int i=0;i<numberOfStatefulActors;++i){
            String actorId = UUID.randomUUID().toString();
            ActorRef statefulActor = context().system().actorOf(Props.create(StatefulActor.class,actorId,expaectedMessages));
            log.debug("created actor "+actorId);
            actors.put(actorId,statefulActor);
        }
    }
}
