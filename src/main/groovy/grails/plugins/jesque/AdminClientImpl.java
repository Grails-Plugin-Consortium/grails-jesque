package grails.plugins.jesque;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.admin.AbstractAdminClient;
import net.greghaines.jesque.admin.AdminClient;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class AdminClientImpl extends AbstractAdminClient implements AdminClient {

    private final Pool<Jedis> jedisPool;

    public AdminClientImpl(final Config config, final Pool<Jedis> jedisPool) {
        super(config);
        this.jedisPool = jedisPool;
    }

    @Override
    protected void doPublish(String channel, String jobJson) throws Exception {
        doPublish(jedisPool.getResource(), getNamespace(), channel, jobJson);
    }

    @Override
    public void end() {
        if(jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }
    }
}
