package ycl.map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.run.ycl.utils.ConvertUtils;
import com.run.ycl.utils.DmUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;

/**
 * 通用格转
 * DataMapping.xml
 */
public class CommonDataMappingMapper implements MapFunction<String, JSONObject> {

    private String protocolName;

    public CommonDataMappingMapper() {
    }
    public CommonDataMappingMapper(String protocolName) {
        this.protocolName = protocolName;
    }

    @Override
    public JSONObject map(String value) throws Exception {
        JSONObject json = ConvertUtils.convertString2Json(value, "");

        JSONObject dataMappingJson = JSONObject.parseObject("{\"IsDefault\":\"\",\"ModifyTime\":\"\",\"CreateTime\":\"\",\"NormalizedField\":[{\"Expression\":{\"Function\":\"dm_copy\",\"Param\":{\"Value\":\"B000001\",\"Name\":\"element\"}},\"Element\":\"B000011\"},{\"Expression\":{\"Function\":\"dm_map\",\"Param\":[{\"Value\":\"H010002\",\"Name\":\"element\"},{\"Value\":\"SEX_TYPE_MAP\",\"Name\":\"codemap\"}]},\"Element\":\"H010002\"},{\"Expression\":{\"Function\":\"dm_time\",\"Param\":{\"Value\":\"B050014\",\"Fmt\":\"%Y-%m-%d %H:%M:%S\",\"Name\":\"element\"}},\"Element\":\"B050014\"},{\"Expression\":{\"Function\":\"dm_assignment\",\"Param\":{\"Value\":\"C15\",\"Name\":\"const\"}},\"Element\":\"H010002\"},{\"Expression\":{\"Function\":\"dm_assignment\",\"Param\":{\"Value\":110000,\"Name\":\"const\"}},\"Element\":\"F010008\"},{\"Expression\":{\"Function\":\"dm_assignment\",\"Param\":{\"Value\":904,\"Name\":\"const\"}},\"Element\":\"B050016\"}],\"CreateUserid\":\"\",\"DDataSet\":\"WA_YCL_TEST_0001\",\"NameSpace\":\"JRPT003\",\"SDataSet\":\"WA_YCL_TEST_0001\",\"Id\":\"\",\"Name\":\"string\"}");
        JSONArray normalizedFieldArray = dataMappingJson.getJSONArray("NormalizedField");
        normalizedFieldArray.forEach(normalizedField -> {
            JSONObject normalizedFieldJson = (JSONObject) normalizedField;
            handle(json, normalizedFieldJson);
        });
        return json;
    }

    //DV_MUST, DV_MAC, DV_IP, DV_BSID, DV_CARD, DV_NUMBER, DV_LON, DV_LAT, DV_IN, DV_NOT_IN, DV_DATE, DV_CAR_NUMBER, DV_MOBILE, DV_IMSI, DV_IMEI,

    //DU_NORMAL, DU_MAC, DU_MOBILE, DU_URL, DU_CARD, DU_TRIM_DELIM, DU_BSID,
    private void handle(JSONObject value, JSONObject normalizedField) {
        JSONObject functionJson = normalizedField.getJSONObject("Expression");
        if (null != functionJson) {
            String functionName = functionJson.getString("Function");
            switch (functionName) {
                case "empty" :
                    break;
                case "dm_copy" :
                    DmUtils.dmCopy(value, functionJson);
                    break;
                case "dm_map" :
                    DmUtils.dmMap(value, functionJson, new HashMap<String, String>());
                    break;
                case "dm_assignment" :
                    DmUtils.dmAssignment(value, functionJson);
                    break;
                case "dm_time" :
                    DmUtils.dmTime(value, functionJson);
                    break;
                case "dm_timeV2" :
                    DmUtils.dmTimeV2(value, functionJson);
                    break;
                case "dm_dateV2" :
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
