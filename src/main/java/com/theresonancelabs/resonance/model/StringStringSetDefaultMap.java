package com.theresonancelabs.resonance.model;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.theresonancelabs.resonance.util.AssertionUtils;
import com.theresonancelabs.resonance.util.DynamoDBUtils;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

import java.util.*;

public class StringStringSetDefaultMap {
    private final Map<String, TreeSet<String>> valueMap;
    private final Map<String, TreeSet<String>>  defaultMap;
    private final Set<String> defaultKeySet;

    public StringStringSetDefaultMap(CaseInsensitiveMap<String, Set<String>> valueMap, Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(valueMap != null && valueMap.size() > 0 , "value map should not be null or empty", context);
        this.valueMap = new CaseInsensitiveMap<>();
        for(String key : valueMap.keySet()) {
            TreeSet<String> currValue = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            currValue.addAll(valueMap.get(key));
            AssertionUtils.throwRuntimeExceptionOnCondition(this.valueMap.put(key, currValue) == null, "existing item should be null", context);
        }

        CaseInsensitiveMap<TreeSet<String> /*value*/, Set<String> /*set of keys with same value*/> valueKeySetMap = new CaseInsensitiveMap<>();
        TreeMap<Integer /* count*/, Set<TreeSet<String>> /*set of values whose value set size is count*/> valueCountMap = new TreeMap<>();
        for (String key : this.valueMap.keySet()) {

            // TreeSet with case insensitive order so that sets such as ["R&B/Soul"] and ["R&B/soul"] are treated as equal
            TreeSet<String> caseInSensitiveValue = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            caseInSensitiveValue.addAll(this.valueMap.get(key));

            if (!valueKeySetMap.containsKey(caseInSensitiveValue)) {
                valueKeySetMap.put(caseInSensitiveValue, new HashSet<>());
            }

            valueKeySetMap.get(caseInSensitiveValue).add(key);
            if (valueCountMap.containsKey(valueKeySetMap.get(caseInSensitiveValue).size()-1)) {
                valueCountMap.get(valueKeySetMap.get(caseInSensitiveValue).size()-1).remove(caseInSensitiveValue);
            }

            if (!valueCountMap.containsKey(valueKeySetMap.get(caseInSensitiveValue).size())) {
                valueCountMap.put(valueKeySetMap.get(caseInSensitiveValue).size(), new HashSet<>());
            }
            valueCountMap.get(valueKeySetMap.get(caseInSensitiveValue).size()).add(caseInSensitiveValue);
        }

        int defaultCount = valueCountMap.descendingKeySet().iterator().next();
        Set<TreeSet<String>> defaultValueSet = valueCountMap.get(defaultCount);
        defaultKeySet = Collections.unmodifiableSet(valueKeySetMap.get(defaultValueSet.iterator().next()));
        CaseInsensitiveMap<String, TreeSet<String>> defaultValueMap = new CaseInsensitiveMap<>();
        for (String key : this.valueMap.keySet()) {
            if (defaultKeySet.contains(key)) {
                if (defaultValueMap.containsKey("default")) {
                    AssertionUtils.throwRuntimeExceptionOnCondition(defaultValueMap.get("default").equals(this.valueMap.get(key)), "default key value should equal expected", context, "valueMap", this.valueMap, "defaultValueMap", defaultValueMap, "key", key, "defaultKeySet", defaultKeySet);
                } else {
                    defaultValueMap.put("default", this.valueMap.get(key));
                }
            } else {
                defaultValueMap.put(key, this.valueMap.get(key));
            }
        }
        this.defaultMap = Collections.unmodifiableMap(defaultValueMap);
    }

    public StringStringSetDefaultMap(AttributeValue stringAttributeValueMapAttrVal, Context context) {
        AssertionUtils.throwRuntimeExceptionOnCondition(stringAttributeValueMapAttrVal != null, "stringAttributeValueMapAttrVal should not be null", context);
        AssertionUtils.throwRuntimeExceptionOnCondition(stringAttributeValueMapAttrVal.getM() != null && stringAttributeValueMapAttrVal.getM().size() > 0, "stringAttributeValueMap should not be null or empty", context);
        Map<String, AttributeValue> stringAttributeValueMap = new HashMap<>(stringAttributeValueMapAttrVal.getM());

        Set<String> defaultSet = DynamoDBUtils.getStringSetOrThrow(stringAttributeValueMap, "defaultSet", context);
        stringAttributeValueMap.remove("defaultSet");
        Set<String> defaultValueSet = DynamoDBUtils.getStringSetOrThrow(stringAttributeValueMap, "default", context);
        stringAttributeValueMap.remove("default");
        TreeSet<String> defaultValueTreeSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        defaultValueTreeSet.addAll(defaultValueSet);

        CaseInsensitiveMap<String, TreeSet<String>> defaultValueMap = new CaseInsensitiveMap<>();
        CaseInsensitiveMap<String, TreeSet<String>> valueMap = new CaseInsensitiveMap<>();
        defaultValueMap.put("default", defaultValueTreeSet);
        for (String key : stringAttributeValueMap.keySet()) {
            Set<String> currValueSet = DynamoDBUtils.getStringSetOrThrow(stringAttributeValueMap, key, context);
            TreeSet<String> currValueTreeSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            currValueTreeSet.addAll(currValueSet);
            defaultValueMap.put(key, currValueTreeSet);
            valueMap.put(key, currValueTreeSet);
        }

        for (String defaultKey : defaultSet) {
            valueMap.put(defaultKey, defaultValueTreeSet);
        }
        this.defaultKeySet = Collections.unmodifiableSet(defaultSet);
        this.defaultMap = Collections.unmodifiableMap(defaultValueMap);
        this.valueMap = Collections.unmodifiableMap(valueMap);
    }

    public AttributeValue getMapAttributeValue(Context context) {
        Map<String, AttributeValue> attrValMap = new HashMap<>();
        attrValMap.put("defaultSet", new AttributeValue().withSS(defaultKeySet));
        for (String key : this.defaultMap.keySet()) {
            attrValMap.put(key, DynamoDBUtils.getAttrValfromStringSet(defaultMap.get(key), context));
        }
        return new AttributeValue().withM(attrValMap);
    }

    public CaseInsensitiveMap<String, Set<String>> getValueMap() {
        return new CaseInsensitiveMap<>(valueMap);
    }

    public CaseInsensitiveMap<String, Set<String>> getDefaultMap() {
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
        StringStringSetDefaultMap that = (StringStringSetDefaultMap) o;
        return Objects.equals(valueMap, that.valueMap) &&
                Objects.equals(defaultMap, that.defaultMap) &&
                Objects.equals(defaultKeySet, that.defaultKeySet);
    }

    @Override
    public int hashCode() {

        return Objects.hash(valueMap, defaultMap, defaultKeySet);
    }
}
