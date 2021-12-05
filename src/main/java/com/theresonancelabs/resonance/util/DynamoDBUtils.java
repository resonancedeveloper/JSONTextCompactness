package com.theresonancelabs.resonance.util;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import java.nio.ByteBuffer;
import java.util.*;

public class DynamoDBUtils {
    public static String getStringValueOrThrow(final Map<String, AttributeValue> dynamoDBItem, String attribute, Context context) {
        if (dynamoDBItem.containsKey(attribute)) {
            return dynamoDBItem.get(attribute).getS().trim();
        }

        RuntimeException ex = new RuntimeException("key not found in the record");
        context.getLogger().log(context.getAwsRequestId()+":"+attribute+" key not found in the record. dynamoDBItem: "+dynamoDBItem+"exception"+ex);
        throw ex;
    }

    public static long getLongValueOrThrow(final Map<String, AttributeValue> dynamoDBItem, String attribute, Context context) {
        if (dynamoDBItem.containsKey(attribute)) {
            return Long.parseLong(dynamoDBItem.get(attribute).getN());
        }

        RuntimeException ex = new RuntimeException("key not found in the record");
        context.getLogger().log(context.getAwsRequestId()+": "+attribute+" key not found in the record. dynamoDBItem: "+ dynamoDBItem+ ", exception: "+ ex);
        throw ex;
    }

    public static int getIntValueOrThrow(final Map<String, AttributeValue> dynamoDBItem, String attribute, Context context) {
        if (dynamoDBItem.containsKey(attribute)) {
            return Integer.parseInt(dynamoDBItem.get(attribute).getN());
        }

        RuntimeException ex = new RuntimeException("key not found in the record");
        context.getLogger().log(context.getAwsRequestId()+": "+attribute+" key not found in the record. dynamoDBItem: "+ dynamoDBItem+ ", exception: "+ ex);
        throw ex;
    }

    public static AttributeValue getAttrValfromStringList(List<String> stringList, Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(stringList != null, "List for list attribute value cannot be empty", context);
        AttributeValue listAttrValue = new AttributeValue();
        List<AttributeValue> attrValList = new ArrayList<>();
        for (String currVal : stringList) {
            attrValList.add(getStringAttributeValue(currVal, context));
        }
        listAttrValue.withL(attrValList);
        return listAttrValue;
    }

    public static AttributeValue getAttrValfromStringSet(Set<String> stringSet, Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(stringSet != null && stringSet.size() > 0, "set for string set attribute value cannot be empty", context);

        for (String s : stringSet) {
            AssertionUtils.throwIfStringIsNullOrBlank(s, "stringSet attrVal element cannot be null or empty", context);
        }

        AttributeValue stringSetAttrValue = new AttributeValue();
        stringSetAttrValue.withSS(stringSet);
        return stringSetAttrValue;
    }

    public static AttributeValue getStringAttributeValue(String value, Context context) {
        AssertionUtils.throwIfStringIsNullOrBlank(value, context.getAwsRequestId()+": value must not be null for string attribute value", context);
        AttributeValue attributeValue = new AttributeValue();
        attributeValue.withS(value);
        return attributeValue;
    }

    public static AttributeValue getLongAttributeValue(Long value, Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(value != null, "value should not be null", context);
        AttributeValue attributeValue = new AttributeValue();
        attributeValue.withN(Long.toString(value));
        return attributeValue;
    }

    public static AttributeValue getDoubleAttributeValue(Double value, Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(value != null, "value should not be null", context);
        AttributeValue attributeValue = new AttributeValue();
        attributeValue.withN(Double.toString(value));
        return attributeValue;
    }

    public static AttributeValue getBooleanAttributeValue(String value , Context context) {
        AssertionUtils.throwIfStringIsNullOrBlank(value, context.getAwsRequestId()+": value must not be null for boolean attribute value", context);
        AttributeValue attributeValue = new AttributeValue();
        attributeValue.withBOOL(Boolean.parseBoolean(value));
        return attributeValue;
    }

    public static AttributeValue getBooleanAttributeValue(Boolean value , Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(value != null, context.getAwsRequestId()+": value must not be null for boolean attribute value", context);
        AttributeValue attributeValue = new AttributeValue();
        attributeValue.withBOOL(value);
        return attributeValue;
    }

    public static AttributeValue getBinaryAttributeValue(byte[] value , Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(value != null && value.length > 0, context.getAwsRequestId()+": value must not be null for binary attribute value", context);
        AttributeValue attributeValue = new AttributeValue();
        attributeValue.withB(ByteBuffer.wrap(value));
        return attributeValue;
    }

    public static List<String> getStringListOrThrow(final Map<String, AttributeValue> dynamoDBItem, String attribute, Context context) {
        if (dynamoDBItem.containsKey(attribute) && dynamoDBItem.get(attribute).getL() != null) {
            List<String> stringList = new ArrayList<>();

            List<AttributeValue> attributeValueList = dynamoDBItem.get(attribute).getL();
            for (AttributeValue attributeValue : attributeValueList) {
                if (attributeValue.getS() == null) {
                    RuntimeException ex = new RuntimeException("attributeValueList contains a non string value");
                    context.getLogger().log(context.getAwsRequestId()+": "+attribute+"  value list contains a non string value. attributeValue: "+attributeValue+ ", dynamoDBItem: "+dynamoDBItem+" , exception: "+ ex);
                    throw ex;
                }

                stringList.add(attributeValue.getS());
            }

            return stringList;
        }

        RuntimeException ex = new RuntimeException("key not found in the record");
        context.getLogger().log(context.getAwsRequestId()+": "+attribute+" key not found in the record. dynamoDBItem: "+ dynamoDBItem+ ", exception: "+ ex);
        throw ex;
    }

    public static List<Object> getTypeListOrThrow(final Map<String, AttributeValue> dynamoDBItem, String attribute, Context context) {
        if (dynamoDBItem.containsKey(attribute) && dynamoDBItem.get(attribute).getL() != null) {
            List<Object> valueList = new ArrayList<>();

            List<AttributeValue> attributeValueList = dynamoDBItem.get(attribute).getL();
            for (AttributeValue attributeValue : attributeValueList) {
                if (attributeValue.getS() != null) {
                    valueList.add(attributeValue.getS());
                } else if (attributeValue.getNULL() != null && attributeValue.getNULL()) {
                    String value = null;
                    valueList.add(value);
                } else if (attributeValue.getN() != null) {
                    Double value = Double.parseDouble(attributeValue.getN());
                    valueList.add(value);
                } else if (attributeValue.getBOOL() != null) {
                    Boolean value = attributeValue.getBOOL().booleanValue();
                    valueList.add(value);
                } else if (attributeValue.getB() != null) {
                    byte[] byteArr = attributeValue.getB().array();
                    valueList.add(byteArr);
                } else {
                    RuntimeException ex = new RuntimeException("unexpected value type in list");
                    context.getLogger().log(context.getAwsRequestId()+": "+attribute+"  valueunexpected value type in list. attributeValue: "+attributeValue+ ", dynamoDBItem: "+dynamoDBItem+" , exception: "+ ex);
                    throw ex;
                }
            }

            return valueList;
        }

        RuntimeException ex = new RuntimeException("key not found in the record");
        context.getLogger().log(context.getAwsRequestId()+": "+attribute+" key not found in the record. dynamoDBItem: "+ dynamoDBItem+ ", exception: "+ ex);
        throw ex;
    }

    public static List<Map<String, Object>> getTypeListMapOrThrow(final Map<String, AttributeValue> dynamoDBItem, String attribute, Context context) {
        if (dynamoDBItem.containsKey(attribute) && dynamoDBItem.get(attribute).getL() != null) {
            List<Map<String, Object>> valueList = new ArrayList<>();

            List<AttributeValue> attributeValueList = dynamoDBItem.get(attribute).getL();
            for (AttributeValue attributeValue : attributeValueList) {
                if (attributeValue.getM() == null || attributeValue.getM().size() == 0) {
                    RuntimeException ex = new RuntimeException("unexpected value type in list");
                    context.getLogger().log(context.getAwsRequestId()+": "+attribute+"  valueunexpected value type in list. attributeValue: "+attributeValue+ ", dynamoDBItem: "+dynamoDBItem+" , exception: "+ ex);
                    throw ex;
                }
                Map<String, AttributeValue> valueMap = attributeValue.getM();
                Map<String, Object> valueResultMap = new HashMap<>();
                for (String attributeName : valueMap.keySet()) {
                    AttributeValue value = valueMap.get(attributeName);
                    if (value.getS() != null) {
                        valueResultMap.put(attributeName, value.getS());
                    } else if (value.getNULL() != null && value.getNULL()) {
                        valueResultMap.put(attributeName, null);
                    } else if (value.getN() != null) {
                        Double doubleValue = Double.parseDouble(value.getN());
                        valueResultMap.put(attributeName, doubleValue);
                    } else if (value.getBOOL() != null) {
                        valueResultMap.put(attributeName, value.getBOOL().booleanValue());
                    } else if (value.getB() != null) {
                        byte[] byteArr = value.getB().array();
                        valueResultMap.put(attributeName, byteArr);
                    } else {
                        RuntimeException ex = new RuntimeException("unexpected value type in list");
                        context.getLogger().log(context.getAwsRequestId()+": "+attribute+"  valueunexpected value type in list. attributeValue: "+attributeValue+ ", dynamoDBItem: "+dynamoDBItem+" , exception: "+ ex);
                        throw ex;
                    }
                }
                valueList.add(valueResultMap);
            }

            return valueList;
        }

        RuntimeException ex = new RuntimeException("key not found in the record");
        context.getLogger().log(context.getAwsRequestId()+": "+attribute+" key not found in the record. dynamoDBItem: "+ dynamoDBItem+ ", exception: "+ ex);
        throw ex;
    }

    public static Set<String> getStringSetOrThrow(final Map<String, AttributeValue> dynamoDBItem, String attribute, Context context) {
        if (!dynamoDBItem.containsKey(attribute) || dynamoDBItem.get(attribute).getSS() == null) {
            RuntimeException ex = new RuntimeException("stringSet value is null or not found in the record");
            context.getLogger().log(context.getAwsRequestId()+": "+attribute+" stringSet value is null  in the record. dynamoDBItem: "+ dynamoDBItem+ ", exception: "+ ex);
            throw ex;
        }

        return getStringSetOrNull(dynamoDBItem, attribute, context);
    }

    public static Set<String> getStringSetOrNull(final Map<String, AttributeValue> dynamoDBItem, String attribute, Context context) {
        if (dynamoDBItem.containsKey(attribute)) {
            if (dynamoDBItem.get(attribute).getSS() == null) {
                RuntimeException ex = new RuntimeException("stringSet value is null in the record");
                context.getLogger().log(context.getAwsRequestId()+": "+attribute+" stringSet value is null  in the record. dynamoDBItem: "+ dynamoDBItem+ ", exception: "+ ex);
                throw ex;
            }

            Set<String> stringSet= new HashSet<>();

            for (String stringValue : dynamoDBItem.get(attribute).getSS()) {
                if (stringSet.contains(stringValue)) {
                    RuntimeException ex = new RuntimeException("duplicate value in stringSet");
                    context.getLogger().log(context.getAwsRequestId()+": "+attribute+" duplicate value in stringSet. dynamoDBItem: "+ dynamoDBItem+ ", exception: "+ ex);
                    throw ex;
                }
                stringSet.add(stringValue);
            }

            return stringSet;
        }
        return null;
    }

    public static boolean getBooleanValueOrThrow(final Map<String, AttributeValue> dynamoDBItem, String attribute, Context context) {
        if (dynamoDBItem.containsKey(attribute) && dynamoDBItem.get(attribute).getBOOL() != null) {
            return dynamoDBItem.get(attribute).getBOOL();
        }

        RuntimeException ex = new RuntimeException("key not found in the record");
        context.getLogger().log(context.getAwsRequestId()+": "+attribute+" key not found in the record. dynamoDBItem: "+ dynamoDBItem+ ", exception: "+ ex);
        throw ex;
    }

    public static AttributeValue getMapAttrValForStringStringMap(Map<String,String> referredKeyMap, Context context) {
        if (referredKeyMap != null && referredKeyMap.size() > 0) {
            AttributeValue referredKeyMapAttrVal = new AttributeValue();

            Map<String, AttributeValue> referredKeyAttrValMap = new HashMap<>();
            for (Map.Entry<String, String> entry : referredKeyMap.entrySet()){
                referredKeyAttrValMap.put(entry.getKey(), new AttributeValue().withS(entry.getValue()));
            }

            referredKeyMapAttrVal.withM(referredKeyAttrValMap);
            return referredKeyMapAttrVal;
        }
        return null;
    }

    public static Map<String,String> getStringStringMapFromAttrValOrNull(Map<String,AttributeValue> userPlayAggregateItemMap, String attribute, Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(userPlayAggregateItemMap != null && userPlayAggregateItemMap.size() > 0 , "itemMap should not be null", context);
        if (!userPlayAggregateItemMap.containsKey(attribute)) {
            return null;
        }
        AttributeValue mapAttrVal = userPlayAggregateItemMap.get(attribute);
        AssertionUtils.throwRuntimeExceptionOnCondition(mapAttrVal != null && mapAttrVal.getM() != null && mapAttrVal.getM().size() != 0, "mapAttrVal should not be null or empty", context, "attribute", attribute, "mapAttrVal", mapAttrVal);

        Map<String, String> stringStringMap = new HashMap<>();
        for (Map.Entry<String, AttributeValue> entry : mapAttrVal.getM().entrySet()) {
            AssertionUtils.throwRuntimeExceptionOnCondition(entry.getValue()!= null && entry.getValue().getS() != null, "entry value should be not null and string type", context);
            stringStringMap.put(entry.getKey(), entry.getValue().getS());
        }
        return stringStringMap;
    }

    public static Map<String,String> getStringStringMapFromAttrValOrThrow(Map<String,AttributeValue> userPlayAggregateItemMap, String attribute, Context context) {
        Map<String, String> stringStringMap = getStringStringMapFromAttrValOrNull(userPlayAggregateItemMap, attribute, context);
        AssertionUtils.throwRuntimeExceptionOnCondition(stringStringMap != null && stringStringMap.size() > 0, "stringStringMap size should be greater than zero", context);
        return stringStringMap;
    }
}
