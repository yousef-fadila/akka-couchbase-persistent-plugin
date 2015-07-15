package couchbase.perf;

import akka.persistence.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


class Evt implements Serializable {

    private final String data;

    public Evt(String data) {
        this.data = data;
    }

    public String getData() {
        return data;
    }
}

class ExampleState implements Serializable {
    private String data = "hgjhjegtfkjhgkjhgkljhggkfhgdkflhjtljy5ulyrlouroiytyiutttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt";
    private int counter=0;
    public ExampleState() {
        this(0);
    }

    public ExampleState(int counter) {
        this.counter = counter;
    }

    public ExampleState copy() {
        return new ExampleState(counter);
    }

    public void update(Evt evt) {

        counter++;
    }



    @Override
    public String toString() {
        return " current counter "+counter;
    }

    public int getCounter() {
        return counter;
    }
}

public class StatefulActor extends UntypedPersistentActor {
    private static final Logger log = LoggerFactory.getLogger(StatefulActor.class);
    public static class Cmd implements Serializable {
        private final String data;

        public Cmd(String data) {
            this.data = data;
        }

        public String getData() {
            return data;
        }
    }
    private final String id;
    private final int expaectedMessages;

    public StatefulActor(String id,int expaectedMessages){
        this.id = id;this.expaectedMessages=expaectedMessages;
    }


    @Override
    public String persistenceId() { return id; }



    private ExampleState state = new ExampleState();

    public int getNumEvents() {
        return state.getCounter();
    }

    @Override
    public void onReceiveRecover(Object msg) {
        log.debug("received recovery message "+msg);
        if (msg instanceof Evt) {
            state.update((Evt) msg);

        } else if (msg instanceof SnapshotOffer) {
            state = (ExampleState)((SnapshotOffer)msg).snapshot();

        } else if(msg instanceof RecoveryCompleted) {

            if(state.getCounter()>0&&state.getCounter()!=expaectedMessages){
               log.warn("Current state after recover is "+state.getCounter()+" instead of "+expaectedMessages);
            }

            super.deleteMessages(Long.MAX_VALUE,true);
            super.deleteSnapshots(new SnapshotSelectionCriteria(Long.MAX_VALUE,System.currentTimeMillis()));
        } else if(msg instanceof RecoveryFailure) {
            log.error("recovery failed", ((RecoveryFailure) msg).cause());
            log.debug("after recovery state is " + state.toString());
            super.deleteMessages(Long.MAX_VALUE, true);
            super.deleteSnapshots(new SnapshotSelectionCriteria(Long.MAX_VALUE,System.currentTimeMillis()));
        }
        else{
            unhandled(msg);
        }
    }

    long duration =0;
    long count = 0;

    @Override
    public void onReceiveCommand(Object msg) {
        log.debug("received message "+msg);
        if (msg.equals("persist")) {
            count++;
            log.debug("received command state is "+state);

            final Evt evt1 = new Evt(id + "-" + getNumEvents());

            long timestamp = System.currentTimeMillis();
            persist(evt1, (Evt evt) -> {
                state.update(evt);
                long endTimestamp = System.currentTimeMillis();
                duration += (endTimestamp - timestamp);
                log.info("persist  took " + (endTimestamp - timestamp));

            });
            long persistTimestamp = System.currentTimeMillis();


        } else if (msg.equals("snap")) {
            // IMPORTANT: create a copy of snapshot
            // because ExampleState is mutable !!!
            long timestamp = System.currentTimeMillis();
            saveSnapshot(state.copy());
            long persistTimestamp = System.currentTimeMillis();
            if(persistTimestamp-timestamp>5) {
                log.warn("snapshot took " + (persistTimestamp - timestamp));
            }
        } else if (msg.equals("delete")) {
            deleteMessages(Long.MAX_VALUE,true);
        } else if (msg.equals("print")) {
            //log.info("**** state is "+state.toString());
            log.info("****average time to persist event "+(count>0?duration/count:0));
        }  else {
            unhandled(msg);
        }
    }
}