package ycl.filter;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.run.ycl.utils.DmUtils;
import com.run.ycl.utils.JsonUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Iterator;

/**
 * 数据清洗
 */
public class CommonDataValidateFilter implements FilterFunction<JSONObject> {

    private String redisHost = "192.168.244.100";

    @Override
    public boolean filter(JSONObject valueJson) throws Exception {
        boolean res = true;
        String source = "WA_YCL_TEST_0001";
        JSONObject dataValidataJson = JSONObject.parseObject(getString("DataValidata_" + source));

        JSONArray normalizingArray= JsonUtils.getJSONArray(dataValidataJson, "NormalizedField");

        JSONObject dataMappingJson = JSONObject.parseObject("{\"IsDefault\":\"\",\"ModifyTime\":\"\",\"CreateTime\":\"\",\"NormalizedField\":[{\"Expression\":{\"Function\":\"dm_copy\",\"Param\":{\"Value\":\"B000001\",\"Name\":\"element\"}},\"Element\":\"B000011\"},{\"Expression\":{\"Function\":\"dm_map\",\"Param\":[{\"Value\":\"H010002\",\"Name\":\"element\"},{\"Value\":\"SEX_TYPE_MAP\",\"Name\":\"codemap\"}]},\"Element\":\"H010002\"},{\"Expression\":{\"Function\":\"dm_time\",\"Param\":{\"Value\":\"B050014\",\"Fmt\":\"%Y-%m-%d %H:%M:%S\",\"Name\":\"element\"}},\"Element\":\"B050014\"},{\"Expression\":{\"Function\":\"dm_assignment\",\"Param\":{\"Value\":\"C15\",\"Name\":\"const\"}},\"Element\":\"H010002\"},{\"Expression\":{\"Function\":\"dm_assignment\",\"Param\":{\"Value\":110000,\"Name\":\"const\"}},\"Element\":\"F010008\"},{\"Expression\":{\"Function\":\"dm_assignment\",\"Param\":{\"Value\":904,\"Name\":\"const\"}},\"Element\":\"B050016\"}],\"CreateUserid\":\"\",\"DDataSet\":\"WA_YCL_TEST_0001\",\"NameSpace\":\"JRPT003\",\"SDataSet\":\"WA_YCL_TEST_0001\",\"Id\":\"\",\"Name\":\"string\"}");
        JSONArray normalizedFieldArray = dataMappingJson.getJSONArray("NormalizedField");
        Iterator<Object> normalizedFieldIterator =  normalizedFieldArray.iterator();
        while (normalizedFieldIterator.hasNext()) {
            JSONObject normalizedFieldJson = (JSONObject) normalizedFieldIterator.next();
            res = res && validate(valueJson, normalizedFieldJson);

            if (false) {
                //做多项校验时，是否需要做短路运算
                if (!res) {
                    break;
                }
            }
        }

        return res;
    }

    private boolean validate(JSONObject value, JSONObject normalizedField) {
        JSONObject functionJson = normalizedField.getJSONObject("Expression");
        if (null != functionJson) {
            String functionName = functionJson.getString("Function");
            switch (functionName) {
                case "dv_must" :
                    break;
                case "dv_mac" :
                    DmUtils.dmCopy(value, functionJson);
                    break;
                case "dv_bsid" :
                    DmUtils.dmMap(value, functionJson, new HashMap<String, String>());
                    break;
                case "dv_ip" :
                    DmUtils.dmAssignment(value, functionJson);
                    break;
                case "dv_card" :
                    DmUtils.dmTime(value, functionJson);
                    break;
                case "dv_lon" :
                    DmUtils.dmTimeV2(value, functionJson);
                    break;
                case "dv_lat" :
                    DmUtils.dmDateV2(value, functionJson);
                    break;
                case "dv_number" :
                    DmUtils.dmDateAllV2(value, functionJson);
                    break;
                default:
                    break;
            }
        }

        return true;
    }

    private String getString(String key) {
        JedisPoolConfig poolConfig;
        JedisPool pool;

        poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(8);
        poolConfig.setMaxTotal(18);

        pool = new JedisPool(poolConfig, redisHost);
        Jedis jedis = pool.getResource();

        return jedis.get(key);
    }

}
