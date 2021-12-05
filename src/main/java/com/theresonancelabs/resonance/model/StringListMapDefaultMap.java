package com.theresonancelabs.resonance.model;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.theresonancelabs.resonance.util.AssertionUtils;
import com.theresonancelabs.resonance.util.DynamoDBUtils;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class StringListMapDefaultMap {
    private final Map<String, List<Map<String, Object>>> valueMap;
    private final Map<String, List<Map<String, Object>>>  defaultMap;
    private final Set<String> defaultKeySet;

    public StringListMapDefaultMap(CaseInsensitiveMap<String, List<Map<String, Object>>> valueMap, Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(valueMap != null && valueMap.size() > 0 , "value map should not be null or empty", context);
        this.valueMap = Collections.unmodifiableMap(valueMap);

        CaseInsensitiveMap<List<Map<String, Object>> /*value*/, Set<String> /*set of keys with same value*/> valueKeySetMap = new CaseInsensitiveMap<>();
        TreeMap<Integer /* count*/, Set<List<Map<String, Object>>> /*set of values whose value set size is count*/> valueCountMap = new TreeMap<>();
        for (String key : this.valueMap.keySet()) {
            List<Map<String, Object>> value = this.valueMap.get(key);
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
        Set<List<Map<String, Object>>> defaultValueSet = valueCountMap.get(defaultCount);
        defaultKeySet = Collections.unmodifiableSet(valueKeySetMap.get(defaultValueSet.iterator().next()));
        CaseInsensitiveMap<String, List<Map<String, Object>>> defaultValueMap = new CaseInsensitiveMap<>();
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

    public StringListMapDefaultMap(AttributeValue stringAttributeValueMapAttrVal, Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(stringAttributeValueMapAttrVal != null, "stringAttributeValueMapAttrVal should not be null", context);
        AssertionUtils.throwRuntimeExceptionOnCondition(stringAttributeValueMapAttrVal.getM() != null && stringAttributeValueMapAttrVal.getM().size() > 0, "stringAttributeValueMap should not be null or empty", context);
        Map<String, AttributeValue> stringAttributeValueMap = new HashMap<>(stringAttributeValueMapAttrVal.getM());

        Set<String> defaultSet = DynamoDBUtils.getStringSetOrThrow(stringAttributeValueMap, "defaultSet", context);
        stringAttributeValueMap.remove("defaultSet");
        List<Map<String, Object>> defaultValue = DynamoDBUtils.getTypeListMapOrThrow(stringAttributeValueMap, "default", context);
        stringAttributeValueMap.remove("default");

        CaseInsensitiveMap<String, List<Map<String, Object>>> defaultValueMap = new CaseInsensitiveMap<>();
        CaseInsensitiveMap<String, List<Map<String, Object>>> valueMap = new CaseInsensitiveMap<>();
        defaultValueMap.put("default", defaultValue);
        for (String key : stringAttributeValueMap.keySet()) {
            defaultValueMap.put(key, DynamoDBUtils.getTypeListMapOrThrow(stringAttributeValueMap, key, context));
            valueMap.put(key, DynamoDBUtils.getTypeListMapOrThrow(stringAttributeValueMap, key, context));
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
            List<Map<String, Object>> valueList = defaultMap.get(key);
            List<AttributeValue> attributeValueList = new ArrayList<>();
            for (Map<String, Object> valueMap : valueList) {
                Map<String, AttributeValue> attributeValueMap = new HashMap<>();
                for (String attributeName : valueMap.keySet()) {
                    Object attributeValue = valueMap.get(attributeName);
                    if (attributeValue instanceof String && !StringUtils.isBlank((String) attributeValue)) {
                        attributeValueMap.put(attributeName, DynamoDBUtils.getStringAttributeValue((String) attributeValue, context));
                    } else if (attributeValue == null || (attributeValue instanceof String && StringUtils.isBlank((String) attributeValue))) {
                        attributeValueMap.put(attributeName, new AttributeValue().withNULL(true));
                    } else if (attributeValue instanceof Double) {
                        attributeValueMap.put(attributeName, DynamoDBUtils.getDoubleAttributeValue((Double) attributeValue, context));
                    } else if (attributeValue instanceof Boolean) {
                        attributeValueMap.put(attributeName, DynamoDBUtils.getBooleanAttributeValue((Boolean) attributeValue, context));
                    } else if (attributeValue instanceof byte[]) {
                        attributeValueMap.put(attributeName, DynamoDBUtils.getBinaryAttributeValue((byte[]) attributeValue, context));
                    } else {
                        throw new RuntimeException("unexpected type in attrValMap");
                    }
                }
                AssertionUtils.throwRuntimeExceptionOnCondition(attributeValueMap.size() > 0, "attributeValueMap size should be greater than zero", context);
                attributeValueList.add(new AttributeValue().withM(attributeValueMap));
            }
            AssertionUtils.throwRuntimeExceptionOnCondition(attributeValueList.size() > 0, "attribute value list size should be greater than zero", context);
            attrValMap.put(key, new AttributeValue().withL(attributeValueList));
        }
        return new AttributeValue().withM(attrValMap);
    }

    public CaseInsensitiveMap<String, List<Map<String, Object>>> getValueMap() {
        return new CaseInsensitiveMap<>(valueMap);
    }

    public CaseInsensitiveMap<String, List<Map<String, Object>>> getDefaultMap() {
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
        StringListMapDefaultMap that = (StringListMapDefaultMap) o;
        return Objects.equals(valueMap, that.valueMap) &&
                Objects.equals(defaultMap, that.defaultMap) &&
                Objects.equals(defaultKeySet, that.defaultKeySet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(valueMap, defaultMap, defaultKeySet);
    }
}
