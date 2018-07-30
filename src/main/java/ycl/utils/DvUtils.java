package ycl.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 *
 * 数据校验工具类
 *
 * 功能函数
 *
 *
 */
public class DvUtils {

    /**
     * 必填校验
     *
     * <NormalizedField Element="I010005">
     *     <Expression Function="dv_must">
     *         <Param Name="element" Value="I010005"/>
     *     </Expression>
     * </NormalizedField>
     * 必填项校验规则
     *
     * @param valueJson
     * @param dmField
     */
    public static boolean dvMust(JSONObject valueJson, JSONObject dmField) {
        boolean valadateBool = true;
        if (dmField != null) {
            String dmElementKey = dmField.getString("Element");
            JSONObject expressionJson = dmField.getJSONObject("Expression");
            JSONArray paramJsonArray = JsonUtils.getJSONArray(expressionJson, "Param");

            Map<String, String> paramMap = JsonUtils.convertJsonArrayToMap(paramJsonArray, "Name", "Value");
            String elementValue = paramMap.get("element");

            valadateBool = StringUtils.isBlank(valueJson.getString(elementValue));
        }
        return valadateBool;
    }

    /**
     * 必填校验
     *
     * <NormalizedField Element="Z002244">
     *     <Expression Function="dv_mac">
     *         <Param Name="element" Value="Z002244"/>
     *     </Expression>
     * </NormalizedField>
     *
     * mac地址校验规则，支持格式：xx-xx-xx-xx-xx-xx 或者 xx：xx:xx:xx:xx:xx
     *
     * @param valueJson
     * @param dmField
     */
    public static boolean dvMac(JSONObject valueJson, JSONObject dmField) {
        boolean valadateBool = true;
        //   mac地址校验
        //   /(([a-f0-9]{2}:)|([a-f0-9]{2}-)){5}[a-f0-9]{2}/gi
        return valadateBool;
    }
}
