package com.theresonancelabs.resonance.model;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.theresonancelabs.resonance.util.AssertionUtils;
import com.theresonancelabs.resonance.util.DynamoDBUtils;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

import java.util.*;

public class StringNumberDefaultMap<T> {
    private final Map<String, T> valueMap;
    private final Map<String, T>  defaultMap;
    private final Set<String> defaultKeySet;

    public StringNumberDefaultMap(CaseInsensitiveMap<String, T> valueMap, Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(valueMap != null && valueMap.size() > 0 , "value map should not be null or empty", context);
        this.valueMap = Collections.unmodifiableMap(valueMap);

        CaseInsensitiveMap<T /*value*/, Set<String> /*set of keys with same value*/> valueKeySetMap = new CaseInsensitiveMap<>();
        TreeMap<Integer /* count*/, Set<T> /*set of values whose value set size is count*/> valueCountMap = new TreeMap<>();
        for (String key : this.valueMap.keySet()) {
            T value = this.valueMap.get(key);
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
        Set<T> defaultValueSet = valueCountMap.get(defaultCount);
        defaultKeySet = Collections.unmodifiableSet(valueKeySetMap.get(defaultValueSet.iterator().next()));
        CaseInsensitiveMap<String, T> defaultValueMap = new CaseInsensitiveMap<>();
        for (String key : this.valueMap.keySet()) {
            if (defaultKeySet.contains(key)) {
                if (defaultValueMap.containsKey("default")) {
                    AssertionUtils.throwRuntimeExceptionOnCondition(Objects.equals(defaultValueMap.get("default"), this.valueMap.get(key)), "default key value should equal expected", context);
                } else {
                    defaultValueMap.put("default", this.valueMap.get(key));
                }
            } else {
                defaultValueMap.put(key, this.valueMap.get(key));
            }
        }
        this.defaultMap = Collections.unmodifiableMap(defaultValueMap);
    }

    public StringNumberDefaultMap(AttributeValue stringAttributeValueMapAttrVal, Class<T> clazz, Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(stringAttributeValueMapAttrVal != null, "stringAttributeValueMapAttrVal should not be null", context);
        AssertionUtils.throwRuntimeExceptionOnCondition(stringAttributeValueMapAttrVal.getM() != null && stringAttributeValueMapAttrVal.getM().size() > 0, "stringAttributeValueMap should not be null or empty", context);
        Map<String, AttributeValue> stringAttributeValueMap = new HashMap<>(stringAttributeValueMapAttrVal.getM());

        Set<String> defaultSet = DynamoDBUtils.getStringSetOrThrow(stringAttributeValueMap, "defaultSet", context);
        stringAttributeValueMap.remove("defaultSet");
        T defaultValue = null;
        if (clazz.equals(Long.class)) {
            defaultValue = (T) new Long(DynamoDBUtils.getLongValueOrThrow(stringAttributeValueMap, "default", context));
            stringAttributeValueMap.remove("default");
        } else if (clazz.equals(Integer.class)) {
            defaultValue = (T) new Integer(DynamoDBUtils.getIntValueOrThrow(stringAttributeValueMap, "default", context));
            stringAttributeValueMap.remove("default");
        } else {
            throw new RuntimeException("unexpected type for StringNumberDefaultMap");
        }

        CaseInsensitiveMap<String, T> defaultValueMap = new CaseInsensitiveMap<>();
        CaseInsensitiveMap<String, T> valueMap = new CaseInsensitiveMap<>();
        defaultValueMap.put("default", defaultValue);
        for (String key : stringAttributeValueMap.keySet()) {
            if (defaultValue instanceof Long) {
                defaultValueMap.put(key, (T) new Long(DynamoDBUtils.getLongValueOrThrow(stringAttributeValueMap, key, context)));
                valueMap.put(key, (T) new Long(DynamoDBUtils.getLongValueOrThrow(stringAttributeValueMap, key, context)));
            } else if (defaultValue instanceof Integer) {
                defaultValueMap.put(key, (T) new Long(DynamoDBUtils.getIntValueOrThrow(stringAttributeValueMap, key, context)));
                valueMap.put(key, (T) new Long(DynamoDBUtils.getIntValueOrThrow(stringAttributeValueMap, key, context)));
            } else {
                throw new RuntimeException("unexpected type for StringNumberDefaultMap");
            }
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
        T data = valueMap.values().iterator().next();
        for (String key : this.defaultMap.keySet()) {
            if (data instanceof Long) {
                attrValMap.put(key, DynamoDBUtils.getLongAttributeValue((Long)defaultMap.get(key), context));
            } else if (data instanceof Integer) {
                attrValMap.put(key, DynamoDBUtils.getLongAttributeValue(((Integer)defaultMap.get(key)).longValue(), context));
            } else {
                throw new RuntimeException("unexpected type for StringNumberDefaultMap");
            }
        }
        return new AttributeValue().withM(attrValMap);
    }

    public CaseInsensitiveMap<String, T> getValueMap() {
        return new CaseInsensitiveMap<>(valueMap);
    }

    public CaseInsensitiveMap<String, T> getDefaultMap() {
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
        StringNumberDefaultMap that = (StringNumberDefaultMap) o;
        return Objects.equals(valueMap, that.valueMap) &&
                Objects.equals(defaultMap, that.defaultMap) &&
                Objects.equals(defaultKeySet, that.defaultKeySet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(valueMap, defaultMap, defaultKeySet);
    }
}
