package com.theresonancelabs.resonance.model;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.theresonancelabs.resonance.util.AssertionUtils;
import com.theresonancelabs.resonance.util.DynamoDBUtils;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

import java.util.*;

public class StringBooleanDefaultMap {
    private final Map<String, Boolean> valueMap;
    private final Map<String, Boolean>  defaultMap;
    private final Set<String> defaultKeySet;

    public StringBooleanDefaultMap(CaseInsensitiveMap<String, Boolean> valueMap, Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(valueMap != null && valueMap.size() > 0 , "value map should not be null or empty", context);
        this.valueMap = Collections.unmodifiableMap(valueMap);

        CaseInsensitiveMap<Boolean /*value*/, Set<String> /*set of keys with same value*/> valueKeySetMap = new CaseInsensitiveMap<>();
        TreeMap<Integer /* count*/, Set<Boolean> /*set of values whose value set size is count*/> valueCountMap = new TreeMap<>();
        for (String key : this.valueMap.keySet()) {
            Boolean value = this.valueMap.get(key);
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
        Set<Boolean> defaultValueSet = valueCountMap.get(defaultCount);
        defaultKeySet = Collections.unmodifiableSet(valueKeySetMap.get(defaultValueSet.iterator().next()));
        CaseInsensitiveMap<String, Boolean> defaultValueMap = new CaseInsensitiveMap<>();
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

    public StringBooleanDefaultMap(AttributeValue stringAttributeValueMapAttrVal, Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(stringAttributeValueMapAttrVal != null, "stringAttributeValueMapAttrVal should not be null", context);

        AssertionUtils.throwRuntimeExceptionOnCondition(stringAttributeValueMapAttrVal.getM() != null && stringAttributeValueMapAttrVal.getM().size() > 0, "stringAttributeValueMap should not be null or empty", context);
        Map<String, AttributeValue> stringAttributeValueMap = new HashMap<>(stringAttributeValueMapAttrVal.getM());

        Set<String> defaultSet = DynamoDBUtils.getStringSetOrThrow(stringAttributeValueMap, "defaultSet", context);
        stringAttributeValueMap.remove("defaultSet");
        Boolean defaultValue = DynamoDBUtils.getBooleanValueOrThrow(stringAttributeValueMap, "default", context);
        stringAttributeValueMap.remove("default");

        CaseInsensitiveMap<String, Boolean> defaultValueMap = new CaseInsensitiveMap<>();
        CaseInsensitiveMap<String, Boolean> valueMap = new CaseInsensitiveMap<>();
        defaultValueMap.put("default", defaultValue);
        for (String key : stringAttributeValueMap.keySet()) {
            defaultValueMap.put(key, DynamoDBUtils.getBooleanValueOrThrow(stringAttributeValueMap, key, context));
            valueMap.put(key, DynamoDBUtils.getBooleanValueOrThrow(stringAttributeValueMap, key, context));
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
            attrValMap.put(key, DynamoDBUtils.getBooleanAttributeValue(defaultMap.get(key), context));
        }
        return new AttributeValue().withM(attrValMap);
    }

    public CaseInsensitiveMap<String, Boolean> getValueMap() {
        return new CaseInsensitiveMap<>(valueMap);
    }

    public CaseInsensitiveMap<String, Boolean> getDefaultMap() {
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
        StringBooleanDefaultMap that = (StringBooleanDefaultMap) o;
        return Objects.equals(valueMap, that.valueMap) &&
                Objects.equals(defaultMap, that.defaultMap) &&
                Objects.equals(defaultKeySet, that.defaultKeySet);
    }

    @Override
    public int hashCode() {

        return Objects.hash(valueMap, defaultMap, defaultKeySet);
    }
}
