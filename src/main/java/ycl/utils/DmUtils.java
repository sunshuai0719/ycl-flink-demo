package ycl.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * 代码格转工具类
 *
 * 功能函数
 *
 *
 * 样例XML
 * <NormalizedField Element="H010003">
 *     <Expression Function="dm_map">
 *         <Param Name="element" Value="Z002526"/>
 *         <Param Name="codemap" Value="CITY_CODE_MAP"/>
 *     </Expression>
 *     <Conditions Rel="OR">
 *         <Condition Key=" H010003" Value ="7" Opr="Neg" Fmt=""/>
 *         <Condition Key=" H010003" Value ="8" Opr="Neg" Fmt=""/>
 *     </Conditions>
 * </NormalizedField>
 */
public class DmUtils {
    /**
     * <NormalizedField Element="Z002619">
     *     <Expression Function="dm_copy">
     *         <Param Name="element" Value="B020007"/>
     *     </Expression>
     * </NormalizedField>
     * 将<Param Name="element" Value="B020007"/>中的Value字段内容
     * 拷贝到
     * <NormalizedField Element="Z002619">中的Element字段。
     * @param valueJson
     * @param dmField
     */
    public static void dmCopy(JSONObject valueJson, JSONObject dmField) {
        if (dmField != null) {
            String dmElementKey = dmField.getString("Element");
            JSONObject conditionsJson = dmField.getJSONObject("Conditions");
            boolean expBool = validateExpression(valueJson, conditionsJson);
            if (expBool) {
                JSONObject expressionJson = dmField.getJSONObject("Expression");
                JSONArray paramJsonArray = JsonUtils.getJSONArray(expressionJson, "Param");

                Map<String, String> paramMap = JsonUtils.convertJsonArrayToMap(paramJsonArray, "Name", "Value");
                String elementValue = paramMap.get("element");

                //处理字段copy
                valueJson.put(dmElementKey, valueJson.getString(elementValue));
            }
        }
    }

    /**
     * 1.字典转换策略(目前仅初步实现了该策略)
     * 2.字段合并
     * 3.字段拆分
     *
     *  <Expression Function="dm_map">
     *      <Param Name="element" Value="Z002526"/>
     *      <Param Name="codemap" Value="CITY_CODE_MAP"/>
     *  </Expression>
     *
     * @param valueJson
     * @param dmField
     * @param map
     */
    public static void dmMap(JSONObject valueJson, JSONObject dmField, Map<String ,String> map) {
        String dmElementKey = dmField.getString("Element");
        JSONObject conditionsJson = dmField.getJSONObject("Conditions");
        boolean expBool = validateExpression(valueJson, conditionsJson);
        if (expBool) {
            JSONObject expressionJson = dmField.getJSONObject("Expression");
            JSONArray paramJsonArray = JsonUtils.getJSONArray(expressionJson, "Param");

            Map<String, String> paramMap = JsonUtils.convertJsonArrayToMap(paramJsonArray, "Name", "Value");
            String elementValue = paramMap.get("element");
            String codeMapValue = paramMap.get("codemap");

            //处理根据map映射的情况
            {
                String mapKey = valueJson.getString(elementValue);
                String mapValue = map.get(mapKey);
                valueJson.put(dmElementKey, mapValue);
            }
        }
    }

    /**
     *
     * <NormalizedField Element="Z002000">
     *     <Expression Function="dm_assignment">
     *         <Param Name="const" Value="email"/>
     *     </Expression>
     * </NormalizedField>
     * 将<Param Name="const" Value="email"/>中的Value
     * 赋值给
     * <NormalizedField Element="Z002000">中的Element字段。(注意：Name为类型const)
     * @param valueJson
     * @param dmField
     */
    public static void dmAssignment(JSONObject valueJson, JSONObject dmField) {
        String dmElementKey = dmField.getString("Element");
        JSONObject conditionsJson = dmField.getJSONObject("Conditions");
        boolean expBool = validateExpression(valueJson, conditionsJson);
        if (expBool) {
            JSONObject expressionJson = dmField.getJSONObject("Expression");
            JSONArray paramJsonArray = JsonUtils.getJSONArray(expressionJson, "Param");

            Map<String, String> paramMap = JsonUtils.convertJsonArrayToMap(paramJsonArray, "Name", "Value");
            String constValue = paramMap.get("const");

            //处理字段赋值
            valueJson.put(dmElementKey, constValue);
        }
    }

    /**
     * 时间格转策略(回填系统时间，格式转换)，转换后为绝对秒数时间,如果时间字段值是NULL，则默认为系统时间
     *
     * <NormalizedField Element="H010014">
     *     <Expression Function="dm_time">
     *         <Param Name="element" Value="H010014"  Fmt="systemtime"/>
     *     </Expression>
     * </NormalizedField>
     *
     * <NormalizedField Element="I010005">
     *     <Expression Function="dm_time">
     *         <Param Name="element" Value="I010005"  Fmt="%Y%m%d%H%M%S"/>
     *     </Expression>
     * </NormalizedField>
     *
     * 说明：1)<Param Name="element" Value="H010014"  Fmt="systemtime"/>
     *         当Fmt 为“systemtime”时，取当前系统时间的绝对秒数，赋值给<NormalizedField Element="H010014">的Element字段。
     *         注：Fmt 为“systemtime”时，<Param Name="element" Value="H010014"  Fmt="systemtime"/>也可以为<Param  Fmt="systemtime"/>形式。
     *
     *      2)<Param Name="element" Value="I010005"  Fmt="%Y%m%d%H%M%S"/>
     *        当Fmt是格式描述时，将I010005数据按照格式描述转换为绝对秒数，赋值给<NormalizedField Element="I010005">的Element字段。
     *        Fmt为时间格式条件：
     *        systemtime  -- 取系统时间。
     *        %Y -- 2015 （年）
     *        %m -- 01-12 （月）
     *        %d -- 01-31 （日）
     *        %H -- 00-23 （时）
     *        %M -- 00-59 （分）
     *        %S -- 00-59   （秒）
     *
     * @param valueJson
     * @param dmField
     */
    public static void dmTime(JSONObject valueJson, JSONObject dmField) {
        String dmElementKey = dmField.getString("Element");
        JSONObject conditionsJson = dmField.getJSONObject("Conditions");
        boolean expBool = validateExpression(valueJson, conditionsJson);
        if (expBool) {
            JSONObject expressionJson = dmField.getJSONObject("Expression");
            JSONArray paramJsonArray = JsonUtils.getJSONArray(expressionJson, "Param");

            Map<String, String> paramMap = JsonUtils.convertJsonArrayToMap(paramJsonArray, "Fmt", "Value");

            String fmtValue = paramMap.get("systemtime");
            //处理时间字段
            if (StringUtils.isNotBlank(fmtValue)) {
                valueJson.put(dmElementKey, LocalDateTime.now().toEpochSecond(ZoneOffset.of("+8")));
            } else {
                try {
                    String key = paramMap.get(fmtValue);
                    String dateTimeStr = valueJson.getString(key);
                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                    valueJson.put(dmElementKey, LocalDateTime.parse(dateTimeStr, dateTimeFormatter).toEpochSecond(ZoneOffset.of("+8")));
                } catch (Exception e) {
                    e.printStackTrace();
                    valueJson.put(dmElementKey, LocalDateTime.now().toEpochSecond(ZoneOffset.of("+8")));
                }
            }
        }
    }

    //未实现
    public static void dmTimeV2(JSONObject valueJson, JSONObject dmField) {
        JSONObject conditionsJson = dmField.getJSONObject("Conditions");
        boolean expBool = validateExpression(valueJson, conditionsJson);
        if (expBool) {
            JSONObject expressionJson = dmField.getJSONObject("Expression");
            JSONArray paramJsonArray = JsonUtils.getJSONArray(expressionJson, "Param");
            String elementValue="";
            String codeMapValue="";

            Iterator<Object> paramIterator = paramJsonArray.iterator();
            while (paramIterator.hasNext()) {
                JSONObject paramJson = (JSONObject) paramIterator.next();
                if ("element".equals(paramJson.getString("Name"))) {
                    elementValue = paramJson.getString("Value");
                } else if ("codemap".equals(paramJson.getString("Name"))) {
                    codeMapValue = paramJson.getString("Value");
                }
            }

            //处理根据map映射的情况
            {

            }
        }
    }

    //未实现
    public static void dmDateV2(JSONObject valueJson, JSONObject dmField) {
        JSONObject conditionsJson = dmField.getJSONObject("Conditions");
        boolean expBool = validateExpression(valueJson, conditionsJson);
        if (expBool) {
            JSONObject expressionJson = dmField.getJSONObject("Expression");
            JSONArray paramJsonArray = JsonUtils.getJSONArray(expressionJson, "Param");
            String elementValue="";
            String codeMapValue="";

            Iterator<Object> paramIterator = paramJsonArray.iterator();
            while (paramIterator.hasNext()) {
                JSONObject paramJson = (JSONObject) paramIterator.next();
                if ("element".equals(paramJson.getString("Name"))) {
                    elementValue = paramJson.getString("Value");
                } else if ("codemap".equals(paramJson.getString("Name"))) {
                    codeMapValue = paramJson.getString("Value");
                }
            }

            //处理根据map映射的情况
            {

            }
        }
    }

    //未实现
    public static void dmDateAllV2(JSONObject valueJson, JSONObject dmField) {
        JSONObject conditionsJson = dmField.getJSONObject("Conditions");
        boolean expBool = validateExpression(valueJson, conditionsJson);
        if (expBool) {
            JSONObject expressionJson = dmField.getJSONObject("Expression");
            JSONArray paramJsonArray = JsonUtils.getJSONArray(expressionJson, "Param");
            String elementValue="";
            String codeMapValue="";

            Iterator<Object> paramIterator = paramJsonArray.iterator();
            while (paramIterator.hasNext()) {
                JSONObject paramJson = (JSONObject) paramIterator.next();
                if ("element".equals(paramJson.getString("Name"))) {
                    elementValue = paramJson.getString("Value");
                } else if ("codemap".equals(paramJson.getString("Name"))) {
                    codeMapValue = paramJson.getString("Value");
                }
            }

            //处理根据map映射的情况
            {

            }
        }
    }

    private static boolean validateExpression(JSONObject valueJson, JSONObject conditions) {
        boolean expBool = true;
        if (conditions != null) {
            String expType = conditions.getString("Rel");
            JSONArray conditionArray = JsonUtils.getJSONArray(conditions, "Condition");

            if (conditionArray != null) {
                Iterator<Object> conditionIterator = conditionArray.iterator();
                while (conditionIterator.hasNext()) {
                    JSONObject condition = (JSONObject) conditionIterator.next();
                    if ("OR".equals(expType)) {
                        expBool = false;
                        expBool = expBool || validateCondition(valueJson, condition);
                        if (expBool == true) {
                            //短路
                            break;
                        }
                    } else if ("AND".equals(expType)) {
                        expBool = true;
                        expBool = expBool && validateCondition(valueJson, condition);
                        if (expBool == false) {
                            //短路
                            break;
                        }
                    }
                }
            }
        }
        return expBool;
    }

    private static boolean validateCondition(JSONObject valueJson, JSONObject condition) {
        boolean expBool = true;
        if (condition != null) {
            String key = condition.getString("Key");
            String value = condition.getString("Value");
            String opr = condition.getString("Opr");
            String fmt = condition.getString("Fmt");
            String keyValue = valueJson.getString(key);

            if ("Equal".equals(opr)) {
                //等于
                expBool = value.equals(keyValue);
            } else if ("Neg".equals(opr)) {
                //不等于
                expBool = !value.equals(keyValue);
            }
        }
        return expBool;
    }
}
