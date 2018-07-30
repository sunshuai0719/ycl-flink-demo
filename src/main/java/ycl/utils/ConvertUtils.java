package ycl.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;

public class ConvertUtils {

   /*
   @Test
    public void testconvert() throws Exception {
        //WA_YCL_TEST_0001_PROTO.PB_WA_YCL_TEST_0001.newBuilder();
        String value = "111111\t222222\t张三\t武汉\t武大\t十堰\t1990-1-1\t男";
        JSONObject jsonObject = convertString2Json(value, "");
        System.out.println(jsonObject.toJSONString());

        jsonToProtoBtyeArray("WA_YCL_TEST_0001", jsonObject);
    }
*/
    //采用本地缓存进行优化
    public static JSONObject convertString2Json(String value, String template) throws Exception {
        JSONObject jsonObject = new JSONObject();
        if (StringUtils.isNotBlank(value)) {
            String[] vlueList = value.split("\t");
            template = "{\"ITEM\":[{\"val\":\"\\t\",\"rmk\":\"列分隔符（缺少值时默认为制表符\\t）\",\"key\":\"I010032\"},{\"val\":\"\\n\",\"rmk\":\"行分隔符（缺少默认为换行符）\",\"key\":\"I010033\"},{\"val\":\"WA_YCL_TEST_0001\",\"rmk\":\"数据集代码）\",\"key\":\"A010004\"},{\"val\":\"\",\"rmk\":\"数据来源\",\"key\":\"B050016\"},{\"val\":\"\",\"rmk\":\"数据采集地\",\"key\":\"F010008\"},{\"val\":1,\"rmk\":\"数据起始行，可选项，填写默认为第1行\",\"key\":\"I010038\"},{\"val\":\"UTF-8\",\"rmk\":\"\",\"key\":\"I010039\"}],\"DATASET\":[{\"rmk\":\"BCP数据文件信息\",\"DATA\":{\"ITEM\":[{\"val\":\"\",\"rmk\":\"文件路径\",\"key\":\"H040003\"},{\"val\":\"\",\"rmk\":\"文件名\",\"key\":\"H010020\"},{\"val\":\"\",\"rmk\":\"记录行数\",\"key\":\"I010034\"}]},\"name\":\"WA_COMMON_010014\"},{\"rmk\":\"BCP文件数据结构\",\"DATA\":{\"ITEM\":[{\"val\":\"QQ号码\",\"rmk\":\"\",\"name\":\"QQ\",\"key\":\"B000001\"},{\"val\":\"可能认识人的QQ号码\",\"rmk\":\"\",\"name\":\"RSQQ\",\"key\":\"B000002\"},{\"val\":\"中文名\",\"rmk\":\"\",\"name\":\"ZWM\",\"key\":\"B000003\"},{\"val\":\"居住地\",\"rmk\":\"\",\"name\":\"JZD\",\"key\":\"B000004\"},{\"val\":\"学校\",\"rmk\":\"\",\"name\":\"XX\",\"key\":\"B000005\"},{\"val\":\"家乡\",\"rmk\":\"\",\"name\":\"JX\",\"key\":\"B000006\"},{\"val\":\"出生日期\",\"rmk\":\"\",\"name\":\"CSRQ\",\"key\":\"B000007\"},{\"val\":\"性别\",\"rmk\":\"\",\"name\":\"XB\",\"key\":\"B000008\"}]},\"name\":\"WA_YCL_TEST_0001\"}]}";
            JSONObject templateJson = JSONObject.parseObject(template);
            JSONArray dataSetArray = templateJson.getJSONArray("DATASET");

            Iterator<Object> dataSetIterator = dataSetArray.iterator();
            dataSetIterator.forEachRemaining(dataSet -> {
                JSONObject dataSetJson = (JSONObject) dataSet;
                if (dataSetJson.getString("name").equals("WA_YCL_TEST_0001")) {
                    JSONObject contentJson = dataSetJson.getJSONObject("DATA");
                    Iterator<Object> itemItertor = contentJson.getJSONArray("ITEM").iterator();

                    int i = 0;
                    while (itemItertor.hasNext()) {
                        JSONObject itemJson = (JSONObject) itemItertor.next();
                        jsonObject.put(itemJson.getString("key"), vlueList[i++]);
                    };
                }
            });
        } else {
            throw new Exception("对象转换异常！");
        }
        return jsonObject;
    }

    public static byte[] jsonToProtoBtyeArray(String protocolName, JSONObject json) {
        if (json == null || StringUtils.isBlank(protocolName)) {
            return null;
        }
        try {
            //Class<?> clazz = Class.forName("com.run.ycl.protobean." + protocolName +"_PROTO$PB_"+protocolName+"$Builder");
            Class<?> clazz = Class.forName("com.run.ycl.protobean." + protocolName +"_PROTO$PB_"+protocolName);
            Method method = clazz.getDeclaredMethod("newBuilder");
            method.setAccessible(true);

            Message.Builder builder = (Message.Builder) method.invoke(null, null);
            Descriptors.Descriptor descriptor = builder.getDescriptorForType();

            Iterator<Map.Entry<String, Object>> it = json.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Object> field = it.next();
                Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(field.getKey());
                if (fieldDescriptor == null) {
                    //log .debug
                } else {
                    if (!"array".equals("array")) {
                        //如果是数组
                        /**
                         * 需要做什么样的单独处理，有待进一步研究；
                         */
                        //builder.setField(fieldDescriptor, "");
                    } else {
                        //如果是普通类型
                        builder.setField(fieldDescriptor, field.getValue());
                    }
                }
            }

            System.out.println(JsonFormat.printer().print(builder.build()).toString());

            return builder.build().toByteArray();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }
}
