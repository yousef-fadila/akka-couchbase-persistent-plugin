package couchbase;

import akka.persistence.japi.snapshot.JavaSnapshotStoreSpec;
import com.typesafe.config.ConfigFactory;

/**
 * @author Yousef Fadila on 3/10/2015.
 */


public class CouchbaseSnapshotSpecTest extends JavaSnapshotStoreSpec {

    @Override
    public void beforeAll() {
        super.beforeAll();
    }

    @Override
    public void afterAll(){
        super.afterAll();
    }

    public CouchbaseSnapshotSpecTest() {
        super(ConfigFactory.load());
    }




}
