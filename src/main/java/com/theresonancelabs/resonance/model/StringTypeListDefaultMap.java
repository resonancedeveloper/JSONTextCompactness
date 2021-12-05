package com.theresonancelabs.resonance.model;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.theresonancelabs.resonance.util.AssertionUtils;
import com.theresonancelabs.resonance.util.DynamoDBUtils;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class StringTypeListDefaultMap {
    private final Map<String, List<Object>> valueMap;
    private final Map<String, List<Object>>  defaultMap;
    private final Set<String> defaultKeySet;

    public static CaseInsensitiveMap<String, List<Object>> getObjectValueMapFromStringMap(CaseInsensitiveMap<String, List<String>> valueMap) {
        CaseInsensitiveMap<String, List<Object>> objectListMap = new CaseInsensitiveMap<>();
        for (String key : valueMap.keySet()) {
            List<Object> objectList = new ArrayList<Object>(valueMap.get(key));
            objectListMap.put(key, objectList);
        }
        return objectListMap;
    }

    public static CaseInsensitiveMap<String, List<String>> getStringValueMapFromObjectMap(CaseInsensitiveMap<String, List<Object>> valueMap) {
        CaseInsensitiveMap<String, List<String>> stringListMap = new CaseInsensitiveMap<>();
        for (String key : valueMap.keySet()) {
            List<String> stringList = new ArrayList<String>();
            for (Object object : valueMap.get(key)) {
                stringList.add((String) object);
            }
            stringListMap.put(key, stringList);
        }
        return stringListMap;
    }

    public StringTypeListDefaultMap(CaseInsensitiveMap<String, List<Object>> valueMap, Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(valueMap != null && valueMap.size() > 0 , "value map should not be null or empty", context);
        this.valueMap = Collections.unmodifiableMap(valueMap);

        CaseInsensitiveMap<List<Object> /*value*/, Set<String> /*set of keys with same value*/> valueKeySetMap = new CaseInsensitiveMap<>();
        TreeMap<Integer /* count*/, Set<List<Object>> /*set of values whose value set size is count*/> valueCountMap = new TreeMap<>();
        for (String key : this.valueMap.keySet()) {
            List<Object> value = this.valueMap.get(key);
            if (!valueKeySetMap.containsKey(value)) {
                valueKeySetMap.put(value, new HashSet<>());
            }

            valueKeySetMap.get(value).add(key);
            if (valueCountMap.containsKey(valueKeySetMap.get(value).size()-1)) {
                valueCountMap.get(valueKeySetMap.get(value).size()-1).remove(value);
            }

            if (!valueCountMap.containsKey(valueKeySetMap.get(value).size())) {
                valueCountMap.put(valueKeySetMap.get(value).size(), new HashSet<>());
            }
            valueCountMap.get(valueKeySetMap.get(value).size()).add(value);
        }

        int defaultCount = valueCountMap.descendingKeySet().iterator().next();
        Set<List<Object>> defaultValueSet = valueCountMap.get(defaultCount);
        defaultKeySet = Collections.unmodifiableSet(valueKeySetMap.get(defaultValueSet.iterator().next()));
        CaseInsensitiveMap<String, List<Object>> defaultValueMap = new CaseInsensitiveMap<>();
        for (String key : this.valueMap.keySet()) {
            if (defaultKeySet.contains(key)) {
                if (defaultValueMap.containsKey("default")) {
                    AssertionUtils.throwRuntimeExceptionOnCondition(defaultValueMap.get("default").equals(this.valueMap.get(key)), "default key value should equal expected", context);
                } else {
                    defaultValueMap.put("default", this.valueMap.get(key));
                }
            } else {
                defaultValueMap.put(key, this.valueMap.get(key));
            }
        }
        this.defaultMap = Collections.unmodifiableMap(defaultValueMap);
    }

    public StringTypeListDefaultMap(AttributeValue stringAttributeValueMapAttrVal, Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(stringAttributeValueMapAttrVal != null, "stringAttributeValueMapAttrVal should not be null", context);
        AssertionUtils.throwRuntimeExceptionOnCondition(stringAttributeValueMapAttrVal.getM() != null && stringAttributeValueMapAttrVal.getM().size() > 0, "stringAttributeValueMap should not be null or empty", context);
        Map<String, AttributeValue> stringAttributeValueMap = new HashMap<>(stringAttributeValueMapAttrVal.getM());

        Set<String> defaultSet = DynamoDBUtils.getStringSetOrThrow(stringAttributeValueMap, "defaultSet", context);
        stringAttributeValueMap.remove("defaultSet");
        List<Object> defaultValue = DynamoDBUtils.getTypeListOrThrow(stringAttributeValueMap, "default", context);
        stringAttributeValueMap.remove("default");

        CaseInsensitiveMap<String, List<Object>> defaultValueMap = new CaseInsensitiveMap<>();
        CaseInsensitiveMap<String, List<Object>> valueMap = new CaseInsensitiveMap<>();
        defaultValueMap.put("default", defaultValue);
        for (String key : stringAttributeValueMap.keySet()) {
            defaultValueMap.put(key, DynamoDBUtils.getTypeListOrThrow(stringAttributeValueMap, key, context));
            valueMap.put(key, DynamoDBUtils.getTypeListOrThrow(stringAttributeValueMap, key, context));
        }

        for (String defaultKey : defaultSet) {
            valueMap.put(defaultKey, defaultValue);
        }
        this.defaultKeySet = Collections.unmodifiableSet(defaultSet);
        this.defaultMap = Collections.unmodifiableMap(defaultValueMap);
        this.valueMap = Collections.unmodifiableMap(valueMap);
    }

    public AttributeValue getMapAttributeValue(Context context) {
        Map<String, AttributeValue> attrValMap = new HashMap<>();
        attrValMap.put("defaultSet", new AttributeValue().withSS(defaultKeySet));
        for (String key : this.defaultMap.keySet()) {
            List<Object> valueList = defaultMap.get(key);
            List<AttributeValue> attributeValueList = new ArrayList<>();
            for (Object value : valueList) {
                if (value instanceof String && !StringUtils.isBlank((String) value)) {
                    attributeValueList.add(DynamoDBUtils.getStringAttributeValue((String)value, context));
                } else if ( value == null || (value instanceof String && StringUtils.isBlank((String) value))) {
                    attributeValueList.add(new AttributeValue().withNULL(true));
                } else if (value instanceof Double) {
                    attributeValueList.add(DynamoDBUtils.getDoubleAttributeValue((Double)value, context));
                } else if (value instanceof Boolean) {
                    attributeValueList.add(DynamoDBUtils.getBooleanAttributeValue((Boolean)value, context));
                } else if (value instanceof byte[]) {
                    attributeValueList.add(DynamoDBUtils.getBinaryAttributeValue((byte[])value, context));
                } else {
                    throw new RuntimeException("unexpected type in attrValMap");
                }
            }
            AssertionUtils.throwRuntimeExceptionOnCondition(attributeValueList.size() > 0, "attribute value list should size should be greater than zero", context);
            attrValMap.put(key, new AttributeValue().withL(attributeValueList));
        }
        return new AttributeValue().withM(attrValMap);
    }

    public CaseInsensitiveMap<String, List<Object>> getValueMap() {
        return new CaseInsensitiveMap<>(valueMap);
    }

    public CaseInsensitiveMap<String, List<Object>> getDefaultMap() {
        return new CaseInsensitiveMap<>(defaultMap);
    }

    public Set<String> getDefaultKeySet() {
        return defaultKeySet;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StringStringDefaultMap{");
        sb.append("valueMap=").append(valueMap);
        sb.append(", defaultMap=").append(defaultMap);
        sb.append(", defaultKeySet=").append(defaultKeySet);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StringTypeListDefaultMap that = (StringTypeListDefaultMap) o;
        return Objects.equals(valueMap, that.valueMap) &&
                Objects.equals(defaultMap, that.defaultMap) &&
                Objects.equals(defaultKeySet, that.defaultKeySet);
    }

    @Override
    public int hashCode() {

        return Objects.hash(valueMap, defaultMap, defaultKeySet);
    }
}
