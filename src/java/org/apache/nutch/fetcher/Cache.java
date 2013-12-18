package org.apache.nutch.fetcher;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * A simple distributed host cache
 *
 * This should hopefully deal with the problem of multiple servers redirecting to the same host and overriding politeness
 * and potentially allow a simultaneous job to be run at once even with duplicated hosts (with degraded performance)
 */
public class Cache {
  public static final Logger LOG = LoggerFactory.getLogger(Cache.class);
  private MemcachedClient client;
  private String identity;

  Cache(Configuration conf) {
    String server = conf.get("cache.server", null);
    long timeout = conf.getLong("cache.timeout", 100);
    client = null;

    Random random = new Random();
    identity = Long.toString(random.nextLong()) + ":" + System.currentTimeMillis();

    if (server != null && server.contains(":")) {
      try {
        ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();
        builder.setOpTimeout(timeout);

        client = new MemcachedClient(builder.build(), AddrUtil.getAddresses(server));
        LOG.info("Connected to memcached server at " + server);
      } catch (IOException e) {
        LOG.error("Caught exception connecting to memcached ", e);
      }
    }
  }

  public void set(String key, int expires) {
    if (client == null) {
      return;
    }

    try {
      client.set(key, expires, identity);
    } catch (Exception e) {
      LOG.error("Caught exception setting " + key, e);
    }
  }

  public boolean exists(String key) {
    if (client == null) {
      return false;
    }

    try {
      Object o = client.get(key);
      if (o != null && !o.equals(identity)) {
        return true;
      }
    } catch (Exception e) {
      LOG.error("Caught exception getting " + key, e);
    }

    return false;
  }
}
