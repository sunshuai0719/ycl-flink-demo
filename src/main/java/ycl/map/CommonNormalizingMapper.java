package ycl.map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.run.ycl.utils.DmUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;

/**
 * 归一化处理
 * Normalizing.xml
 */
public class CommonNormalizingMapper implements MapFunction<JSONObject, JSONObject> {

    private String protocolName;

    public CommonNormalizingMapper() {
    }
    public CommonNormalizingMapper(String protocolName) {
        this.protocolName = protocolName;
    }

    @Override
    public JSONObject map(JSONObject valueJson) throws Exception {

        JSONObject dataMappingJson = JSONObject.parseObject("{\"IsDefault\":\"\",\"ModifyTime\":\"\",\"CreateTime\":\"\",\"NormalizedField\":[{\"Expression\":{\"Function\":\"dm_copy\",\"Param\":{\"Value\":\"B000001\",\"Name\":\"element\"}},\"Element\":\"B000011\"},{\"Expression\":{\"Function\":\"dm_map\",\"Param\":[{\"Value\":\"H010002\",\"Name\":\"element\"},{\"Value\":\"SEX_TYPE_MAP\",\"Name\":\"codemap\"}]},\"Element\":\"H010002\"},{\"Expression\":{\"Function\":\"dm_time\",\"Param\":{\"Value\":\"B050014\",\"Fmt\":\"%Y-%m-%d %H:%M:%S\",\"Name\":\"element\"}},\"Element\":\"B050014\"},{\"Expression\":{\"Function\":\"dm_assignment\",\"Param\":{\"Value\":\"C15\",\"Name\":\"const\"}},\"Element\":\"H010002\"},{\"Expression\":{\"Function\":\"dm_assignment\",\"Param\":{\"Value\":110000,\"Name\":\"const\"}},\"Element\":\"F010008\"},{\"Expression\":{\"Function\":\"dm_assignment\",\"Param\":{\"Value\":904,\"Name\":\"const\"}},\"Element\":\"B050016\"}],\"CreateUserid\":\"\",\"DDataSet\":\"WA_YCL_TEST_0001\",\"NameSpace\":\"JRPT003\",\"SDataSet\":\"WA_YCL_TEST_0001\",\"Id\":\"\",\"Name\":\"string\"}");
        JSONArray normalizedFieldArray = dataMappingJson.getJSONArray("NormalizedField");
        normalizedFieldArray.forEach(normalizedField -> {
            JSONObject normalizedFieldJson = (JSONObject) normalizedField;
            handle(valueJson, normalizedFieldJson);
        });
        return valueJson;
    }

    //DU_NORMAL, DU_MAC, DU_MOBILE, DU_URL, DU_CARD, DU_TRIM_DELIM, DU_BSID,
    private void handle(JSONObject value, JSONObject normalizedField) {
        JSONObject functionJson = normalizedField.getJSONObject("Expression");
        if (null != functionJson) {
            String functionName = functionJson.getString("Function");
            switch (functionName) {
                case "du_normal" :
                    break;
                case "du_mac" :
                    DmUtils.dmCopy(value, functionJson);
                    break;
                case "du_mobile" :
                    DmUtils.dmMap(value, functionJson, new HashMap<String, String>());
                    break;
                case "du_url" :
                    DmUtils.dmAssignment(value, functionJson);
                    break;
                case "du_card" :
                    DmUtils.dmTime(value, functionJson);
                    break;
                case "du_trim_delim" :
                    DmUtils.dmTimeV2(value, functionJson);
                    break;
                case "du_bsid" :
                    DmUtils.dmDateV2(value, functionJson);
                    break;
                case "dm_dateallV2" :
                    DmUtils.dmDateAllV2(value, functionJson);
                    break;
                default:
                    break;
            }
        }
    }
}
