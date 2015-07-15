package couchbase;

import akka.persistence.japi.journal.JavaJournalSpec;
import com.github.akka.couchbase.AkkaPersistenceImpl;
import com.typesafe.config.ConfigFactory;
import org.junit.runner.RunWith;
import org.scalatest.junit.JUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yousef Fadila on 3/10/2015.
 */

@RunWith(JUnitRunner.class)
public class CouchbaseJournalSpecTest extends JavaJournalSpec {

    private static final Logger log = LoggerFactory.getLogger(CouchbaseJournalSpecTest.class);
    @Override
    public void beforeAll() {
        super.beforeAll();
    }

    @Override
    public void afterAll(){
        super.afterAll();
    }

    public CouchbaseJournalSpecTest() {
        super(ConfigFactory.load());
    }

}
