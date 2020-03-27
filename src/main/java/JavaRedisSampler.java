import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class JavaRedisSampler extends AbstractJavaSamplerClient {
    /**
     * 输出到日志文件
     */
    static org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(JavaRedisSampler.class.getName());

    private SampleResult results;
    private String redisServerList;
    private String method;
    private String key;
    private String value;
    private boolean checkparameter;
    private Set HostandPort ;
    private String response;
    private boolean isCluster;
    private boolean isPersist;
    private long startime;
    private long endtime;
    private StringBuilder ret;
    private Object redisclient ;

    @Override
    public void setupTest(JavaSamplerContext arg0) {

        checkparameter = true;
        HostandPort = new HashSet();
        redisServerList = arg0.getParameter("redisServerList", "");
        method = arg0.getParameter("method", "");
        response = "not found the key";
        isPersist = arg0.getParameter("isPersist", "").equals("true") ? true : false;
        isCluster = arg0.getParameter("isCluster", "").equals("true") ? true : false;
        ret = new StringBuilder();
        Iterator itr = arg0.getParameterNamesIterator();
        while (itr.hasNext()) {
            String element = (String) itr.next();
            if (arg0.getParameter(element, "").equals("")) {
                checkparameter = false;
            }

            String tempString = element + ":" + arg0.getParameter(element, "") + "\n";
            ret.append(tempString);
        }

        String[] servers = redisServerList.split(",", -1);

        for (int i = 0; i < servers.length; i++) {
            String[] hostandport = servers[i].split(":", -1);
            if (!hostandport[0].equals("") && !hostandport[1].equals("")) {
                HostandPort.add(new HostAndPort(hostandport[0], Integer.valueOf(hostandport[1])));
            } else {
                checkparameter = false;
            }
        }
        if (servers.length > 1 && !isCluster) {
            checkparameter = false;
        }
        if (isCluster) {
            redisclient = new JedisCluster(HostandPort);
        } else {
            Iterator iter = HostandPort.iterator();
            HostAndPort first = (HostAndPort) iter.next();
            redisclient = new Jedis(first.getHost(), first.getPort());
        }

    }


    @Override
    public Arguments getDefaultParameters() {
        Arguments params = new Arguments();
        params.addArgument("redisServerList", "127.0.0.1:6379");
        params.addArgument("isCluster", "false");
        params.addArgument("method", "setex|get|ttl...");
        params.addArgument("key", "key");
        params.addArgument("value", "value");
        params.addArgument("isPersist", "false");

        return params;

    }


    @Override
    public SampleResult runTest(JavaSamplerContext arg0) {

        key = arg0.getParameter("key", "") + new BigInteger(130, new SecureRandom()).toString(32);
        value = arg0.getParameter("value", "");
        startime = System.currentTimeMillis();
        results = new SampleResult();
        results.sampleStart();
        results.setSamplerData(ret.toString() + " \n actualkey: " + key);
        // 如果参数有一项为空检查错误，则直接返回错误
        if (!checkparameter) {
            logger.info("fail...");
            results.setSuccessful(false);
            return results;
        }
        if (isCluster) {
            if (method.equals("setex")) {
                response = ((JedisCluster) redisclient).setex(key, 5000, value);
                if (isPersist) {
                    ((JedisCluster) redisclient).persist(key);
                }
            } else if (method.equals("get")) {
                response = ((JedisCluster) redisclient).get(key);
            } else if (method.equals("ttl")) {
                response = String.valueOf(((JedisCluster) redisclient).pttl(key));
            } else {
                logger.info("暂不支持此命令...");
            }

        } else {
            if (method.equals("setex")) {
                response = ((Jedis) redisclient).setex(key, 5000, value);
                if (isPersist) {
                    ((Jedis) redisclient).persist(key);
                } else if (method.equals("get")) {
                    response = ((Jedis) redisclient).get(key);
                } else if (method.equals("ttl")) {
                    response = String.valueOf(((Jedis) redisclient).pttl(key));
                } else {
                    logger.info("暂不支持此命令...");
                }
                if (null == response) {
                    response = "not found the key";
                }
            }
        }
        results.setResponseData(response, "utf-8");
        results.setSuccessful(true);
        results.setResponseCodeOK();
        results.setEndTime(System.currentTimeMillis());
        return results;
    }


    @Override
    public void teardownTest(JavaSamplerContext arg0) {
        results.cleanAfterSample();

    }

}
