package com.theresonancelabs.resonance.model;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.theresonancelabs.resonance.util.EmptyLambdaContext;
import com.theresonancelabs.resonance.util.TemplateUtils;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class TestDefaultMaps {

    private Context context;

    @Before
    public void setup() {
        context = new EmptyLambdaContext();
    }

    @Test
    public void testStringStringDefaultMap() {

        // test default set
        int defaultVal = 100;
        int defaultThreshold = 10;
        Set<String> defaultSet = new HashSet<>();
        Set<String> countryCode = new HashSet<>();
        CaseInsensitiveMap<String, String> countryCodeValueMap = new CaseInsensitiveMap<>();
        for (int i = 0 ; i < 30 ; i++) {
            countryCode.add("cc"+i);
            if (i < defaultThreshold) {
                countryCodeValueMap.put("cc" + i, "value" + defaultVal);
                defaultSet.add("cc" + i);
            } else {
                countryCodeValueMap.put("cc" + i, "value" + i);
            }
        }

        Map<String, AttributeValue> attrValMap = new HashMap<>();
        attrValMap.put("defaultSet", new AttributeValue().withSS(defaultSet));
        attrValMap.put("default", new AttributeValue().withS("value"+defaultVal));
        for (int i = 0 ; i < 30 ; i++) {
            if (defaultSet.contains("cc"+i)) {
                continue;
            }
            attrValMap.put("cc"+i, new AttributeValue().withS("value"+i));
        }

        // serialize
        Assert.assertEquals(new AttributeValue().withM(attrValMap), new StringStringDefaultMap(countryCodeValueMap, context).getMapAttributeValue(context));

        // deserialize
        Assert.assertEquals(countryCodeValueMap, new StringStringDefaultMap(new AttributeValue().withM(attrValMap), context).getValueMap());
    }

    @Test
    public void testStringLongDefaultMap() {

        // test default set
        long defaultVal = 100;
        int defaultThreshold = 10;
        Set<String> defaultSet = new HashSet<>();
        Set<String> countryCode = new HashSet<>();
        CaseInsensitiveMap<String, Long> countryCodeValueMap = new CaseInsensitiveMap<>();
        for (int i = 0 ; i < 30 ; i++) {
            countryCode.add("cc"+i);
            if (i < defaultThreshold) {
                countryCodeValueMap.put("cc" + i, defaultVal);
                defaultSet.add("cc" + i);
            } else {
                countryCodeValueMap.put("cc" + i, (long)i);
            }
        }

        Map<String, AttributeValue> attrValMap = new HashMap<>();
        attrValMap.put("defaultSet", new AttributeValue().withSS(defaultSet));
        attrValMap.put("default", new AttributeValue().withN(Long.toString(defaultVal)));
        for (int i = 0 ; i < 30 ; i++) {
            if (defaultSet.contains("cc"+i)) {
                continue;
            }
            attrValMap.put("cc"+i, new AttributeValue().withN(Long.toString(i)));
        }

        // serialize
        Assert.assertEquals(new AttributeValue().withM(attrValMap), new StringNumberDefaultMap<Long>(countryCodeValueMap, context).getMapAttributeValue(context));

        // deserialize
        Assert.assertEquals(countryCodeValueMap, new StringNumberDefaultMap<Long>(new AttributeValue().withM(attrValMap), Long.class, context).getValueMap());
    }

    @Test
    public void testStringBooleanDefaultMap() {

        // test default set
        boolean defaultVal = true;
        int defaultThreshold = 20;
        Set<String> defaultSet = new HashSet<>();
        Set<String> countryCode = new HashSet<>();
        CaseInsensitiveMap<String, Boolean> countryCodeValueMap = new CaseInsensitiveMap<>();
        for (int i = 0 ; i < 30 ; i++) {
            countryCode.add("cc"+i);
            if (i < defaultThreshold) {
                countryCodeValueMap.put("cc" + i, defaultVal);
                defaultSet.add("cc" + i);
            } else {
                countryCodeValueMap.put("cc" + i, !defaultVal);
            }
        }

        Map<String, AttributeValue> attrValMap = new HashMap<>();
        attrValMap.put("defaultSet", new AttributeValue().withSS(defaultSet));
        attrValMap.put("default", new AttributeValue().withBOOL(defaultVal));
        for (int i = 0 ; i < 30 ; i++) {
            if (defaultSet.contains("cc"+i)) {
                continue;
            }
            attrValMap.put("cc"+i, new AttributeValue().withBOOL(!defaultVal));
        }

        // serialize
        AttributeValue expectedAttrVal = new AttributeValue().withM(attrValMap);
        AttributeValue actualAttrVal = new StringBooleanDefaultMap(countryCodeValueMap, context).getMapAttributeValue(context);
        Assert.assertEquals(expectedAttrVal.getM().size(), actualAttrVal.getM().size());
        for (String key : expectedAttrVal.getM().keySet()) {
            if (expectedAttrVal.getM().get(key).getSS() != null) {
                Assert.assertEquals(new HashSet<>(expectedAttrVal.getM().get(key).getSS()), new HashSet<>(actualAttrVal.getM().get(key).getSS()));
            } else {
                Assert.assertEquals(expectedAttrVal.getM().get(key), actualAttrVal.getM().get(key));
            }
        }

        // deserialize
        Assert.assertEquals(countryCodeValueMap, new StringBooleanDefaultMap(new AttributeValue().withM(attrValMap), context).getValueMap());
    }

    @Test
    public void testStringListMapDefaultMap() {

        // setup the default val
        List<Map<String, Object>> defaultVal = new ArrayList<>();
        Map<String, Object> defaultValMapValue1 = new HashMap<>();
        defaultValMapValue1.put("defaultValMapValue1Key", "defaultValMapValue1Value");
        Map<String, Object> defaultValMapValue2 = new HashMap<>();
        defaultValMapValue2.put("defaultValMapValue2Key", "defaultValMapValue2Value");
        defaultVal.add(defaultValMapValue1);
        defaultVal.add(defaultValMapValue2);

        // default attrVal list
        Map<String, AttributeValue> defaultMapVal1AttrVal = new HashMap<>();
        defaultMapVal1AttrVal.put("defaultValMapValue1Key", new AttributeValue().withS("defaultValMapValue1Value"));
        Map<String, AttributeValue> defaultMapVal2AttrVal = new HashMap<>();
        defaultMapVal2AttrVal.put("defaultValMapValue2Key", new AttributeValue().withS("defaultValMapValue2Value"));
        AttributeValue defaultListAttrVal = new AttributeValue().withL(new AttributeValue().withM(defaultMapVal1AttrVal), new AttributeValue().withM(defaultMapVal2AttrVal));

        // setup the non-default val
        List<Map<String, Object>> nonDefaultVal = new ArrayList<>();
        Map<String, Object> nonDefaultValMapValue1 = new HashMap<>();
        nonDefaultValMapValue1.put("nonDefaultValMapValue1Key", "nonDefaultValMapValue1Value");
        Map<String, Object> nonDefaultValMapValue2 = new HashMap<>();
        nonDefaultValMapValue2.put("nonDefaultValMapValue2Key", "nonDefaultValMapValue2Value");
        nonDefaultVal.add(nonDefaultValMapValue1);
        nonDefaultVal.add(nonDefaultValMapValue2);

        // non-default attrVal list
        Map<String, AttributeValue> nonDefaultMapVal1AttrVal = new HashMap<>();
        nonDefaultMapVal1AttrVal.put("nonDefaultValMapValue1Key", new AttributeValue().withS("nonDefaultValMapValue1Value"));
        Map<String, AttributeValue> nonDefaultMapVal2AttrVal = new HashMap<>();
        nonDefaultMapVal2AttrVal.put("nonDefaultValMapValue2Key", new AttributeValue().withS("nonDefaultValMapValue2Value"));
        AttributeValue nonDefaultListAttrVal = new AttributeValue().withL(new AttributeValue().withM(nonDefaultMapVal1AttrVal), new AttributeValue().withM(nonDefaultMapVal2AttrVal));

        int defaultThreshold = 20;
        Set<String> defaultSet = new HashSet<>();
        Set<String> countryCode = new HashSet<>();
        CaseInsensitiveMap<String, List<Map<String, Object>>> countryCodeValueMap = new CaseInsensitiveMap<>();
        for (int i = 0 ; i < 30 ; i++) {
            countryCode.add("cc"+i);
            if (i < defaultThreshold) {
                countryCodeValueMap.put("cc" + i, defaultVal);
                defaultSet.add("cc" + i);
            } else {
                countryCodeValueMap.put("cc" + i, nonDefaultVal);
            }
        }

        Map<String, AttributeValue> attrValMap = new HashMap<>();
        attrValMap.put("defaultSet", new AttributeValue().withSS(defaultSet));
        attrValMap.put("default", defaultListAttrVal);
        for (int i = 0 ; i < 30 ; i++) {
            if (defaultSet.contains("cc"+i)) {
                continue;
            }
            attrValMap.put("cc"+i, nonDefaultListAttrVal);
        }

        // serialize
        AttributeValue expectedAttrVal = new AttributeValue().withM(attrValMap);
        AttributeValue actualAttrVal = new StringListMapDefaultMap(countryCodeValueMap, context).getMapAttributeValue(context);
        Assert.assertEquals(expectedAttrVal.getM().size(), actualAttrVal.getM().size());
        for (String key : expectedAttrVal.getM().keySet()) {
            if (expectedAttrVal.getM().get(key).getSS() != null) {
                Assert.assertEquals(new HashSet<>(expectedAttrVal.getM().get(key).getSS()), new HashSet<>(actualAttrVal.getM().get(key).getSS()));
            } else {
                Assert.assertEquals(expectedAttrVal.getM().get(key), actualAttrVal.getM().get(key));
            }
        }

        // deserialize
        Assert.assertEquals(countryCodeValueMap, new StringListMapDefaultMap(new AttributeValue().withM(attrValMap), context).getValueMap());
    }

    @Test
    public void testStringStringListDefaultMap() {

        // setup the default val
        List<String> defaultVal = Arrays.asList("string1", "string2");
        AttributeValue defaultListAttrVal = new AttributeValue().withL(new AttributeValue().withS("string1"), new AttributeValue().withS("string2"));

        // setup the non-default val
        List<String> nonDefaultVal = Arrays.asList("non-default-string1", "non-default-string2");
        AttributeValue nonDefaultListAttrVal = new AttributeValue().withL(new AttributeValue().withS("non-default-string1"), new AttributeValue().withS("non-default-string2"));

        int defaultThreshold = 20;
        Set<String> defaultSet = new HashSet<>();
        Set<String> countryCode = new HashSet<>();
        CaseInsensitiveMap<String, List<String>> countryCodeValueMap = new CaseInsensitiveMap<>();
        for (int i = 0 ; i < 30 ; i++) {
            countryCode.add("cc"+i);
            if (i < defaultThreshold) {
                countryCodeValueMap.put("cc" + i, defaultVal);
                defaultSet.add("cc" + i);
            } else {
                countryCodeValueMap.put("cc" + i, nonDefaultVal);
            }
        }

        Map<String, AttributeValue> attrValMap = new HashMap<>();
        attrValMap.put("defaultSet", new AttributeValue().withSS(defaultSet));
        attrValMap.put("default", defaultListAttrVal);
        for (int i = 0 ; i < 30 ; i++) {
            if (defaultSet.contains("cc"+i)) {
                continue;
            }
            attrValMap.put("cc"+i, nonDefaultListAttrVal);
        }

        // serialize
        AttributeValue expectedAttrVal = new AttributeValue().withM(attrValMap);
        AttributeValue actualAttrVal = new StringStringListDefaultMap(countryCodeValueMap, context).getMapAttributeValue(context);
        Assert.assertEquals(expectedAttrVal.getM().size(), actualAttrVal.getM().size());
        for (String key : expectedAttrVal.getM().keySet()) {
            if (expectedAttrVal.getM().get(key).getSS() != null) {
                Assert.assertEquals(new HashSet<>(expectedAttrVal.getM().get(key).getSS()), new HashSet<>(actualAttrVal.getM().get(key).getSS()));
            } else {
                Assert.assertEquals(expectedAttrVal.getM().get(key), actualAttrVal.getM().get(key));
            }
        }

        // deserialize
        Assert.assertEquals(countryCodeValueMap, new StringStringListDefaultMap(new AttributeValue().withM(attrValMap), context).getValueMap());
    }

    @Test
    public void testStringStringMapDefaultMap() {

        // setup the default val
        Map<String, String> defaultVal = new HashMap<>();
        defaultVal.put("defaultValMapValue1Key", "defaultValMapValue1Value");

        // default attrVal list
        Map<String, AttributeValue> defaultMapVal1AttrVal = new HashMap<>();
        defaultMapVal1AttrVal.put("defaultValMapValue1Key", new AttributeValue().withS("defaultValMapValue1Value"));
        AttributeValue defaultAttrVal = new AttributeValue().withM(defaultMapVal1AttrVal);

        // setup the non-default val
        Map<String, String> nonDefaultVal = new HashMap<>();
        nonDefaultVal.put("nonDefaultValMapValue1Key", "nonDefaultValMapValue1Value");

        // non-default attrVal list
        Map<String, AttributeValue> nonDefaultMapVal1AttrVal = new HashMap<>();
        nonDefaultMapVal1AttrVal.put("nonDefaultValMapValue1Key", new AttributeValue().withS("nonDefaultValMapValue1Value"));
        AttributeValue nonDefaultAttrVal = new AttributeValue().withM(nonDefaultMapVal1AttrVal);

        int defaultThreshold = 20;
        Set<String> defaultSet = new HashSet<>();
        Set<String> countryCode = new HashSet<>();
        CaseInsensitiveMap<String, Map<String, String>> countryCodeValueMap = new CaseInsensitiveMap<>();
        for (int i = 0 ; i < 30 ; i++) {
            countryCode.add("cc"+i);
            if (i < defaultThreshold) {
                countryCodeValueMap.put("cc" + i, defaultVal);
                defaultSet.add("cc" + i);
            } else {
                countryCodeValueMap.put("cc" + i, nonDefaultVal);
            }
        }

        Map<String, AttributeValue> attrValMap = new HashMap<>();
        attrValMap.put("defaultSet", new AttributeValue().withSS(defaultSet));
        attrValMap.put("default", defaultAttrVal);
        for (int i = 0 ; i < 30 ; i++) {
            if (defaultSet.contains("cc"+i)) {
                continue;
            }
            attrValMap.put("cc"+i, nonDefaultAttrVal);
        }

        // serialize
        AttributeValue expectedAttrVal = new AttributeValue().withM(attrValMap);
        AttributeValue actualAttrVal = new StringStringMapDefaultMap(countryCodeValueMap, context).getMapAttributeValue(context);
        Assert.assertEquals(expectedAttrVal.getM().size(), actualAttrVal.getM().size());
        for (String key : expectedAttrVal.getM().keySet()) {
            if (expectedAttrVal.getM().get(key).getSS() != null) {
                Assert.assertEquals(new HashSet<>(expectedAttrVal.getM().get(key).getSS()), new HashSet<>(actualAttrVal.getM().get(key).getSS()));
            } else {
                Assert.assertEquals(expectedAttrVal.getM().get(key), actualAttrVal.getM().get(key));
            }
        }

        // deserialize
        Assert.assertEquals(countryCodeValueMap, new StringStringMapDefaultMap(new AttributeValue().withM(attrValMap), context).getValueMap());
    }

    @Test
    public void testStringStringSetDefaultMap() {

        // setup the default val
        Set<String> defaultVal = new HashSet<String>(Arrays.asList("string1", "string2"));
        AttributeValue defaultListAttrVal = new AttributeValue().withSS(defaultVal);

        // setup the non-default val
        Set<String> nonDefaultVal = new HashSet<String>(Arrays.asList("non-default-string1", "non-default-string2"));
        AttributeValue nonDefaultListAttrVal = new AttributeValue().withSS(nonDefaultVal);

        int defaultThreshold = 20;
        Set<String> defaultSet = new HashSet<>();
        Set<String> countryCode = new HashSet<>();
        CaseInsensitiveMap<String, Set<String>> countryCodeValueMap = new CaseInsensitiveMap<>();
        for (int i = 0 ; i < 30 ; i++) {
            countryCode.add("cc"+i);
            if (i < defaultThreshold) {
                countryCodeValueMap.put("cc" + i, defaultVal);
                defaultSet.add("cc" + i);
            } else {
                countryCodeValueMap.put("cc" + i, nonDefaultVal);
            }
        }

        Map<String, AttributeValue> attrValMap = new HashMap<>();
        attrValMap.put("defaultSet", new AttributeValue().withSS(defaultSet));
        attrValMap.put("default", defaultListAttrVal);
        for (int i = 0 ; i < 30 ; i++) {
            if (defaultSet.contains("cc"+i)) {
                continue;
            }
            attrValMap.put("cc"+i, nonDefaultListAttrVal);
        }

        // serialize
        AttributeValue expectedAttrVal = new AttributeValue().withM(attrValMap);
        AttributeValue actualAttrVal = new StringStringSetDefaultMap(countryCodeValueMap, context).getMapAttributeValue(context);
        Assert.assertEquals(expectedAttrVal.getM().size(), actualAttrVal.getM().size());
        for (String key : expectedAttrVal.getM().keySet()) {
            if (expectedAttrVal.getM().get(key).getSS() != null) {
                Assert.assertEquals(new HashSet<>(expectedAttrVal.getM().get(key).getSS()), new HashSet<>(actualAttrVal.getM().get(key).getSS()));
            } else {
                Assert.assertEquals(expectedAttrVal.getM().get(key), actualAttrVal.getM().get(key));
            }
        }

        // deserialize
        Assert.assertEquals(countryCodeValueMap, new StringStringSetDefaultMap(new AttributeValue().withM(attrValMap), context).getValueMap());
    }

    @Test
    public void StringTypeListDefaultMap() {

        // setup the default val - list with a boolean and string
        List<Object> defaultVal = new ArrayList<>();
        defaultVal.add(true);
        defaultVal.add("defaultValMapValue2");

        // default attrVal list
        AttributeValue defaultListAttrVal = new AttributeValue().withL(new AttributeValue().withBOOL(true), new AttributeValue().withS("defaultValMapValue2"));

        // setup the non-default val
        List<Object> nonDefaultVal = new ArrayList<>();
        nonDefaultVal.add(false);
        nonDefaultVal.add("nonDefaultValMapValue2");

        // non-default attrVal list
        Map<String, AttributeValue> nonDefaultMapVal1AttrVal = new HashMap<>();
        AttributeValue nonDefaultListAttrVal = new AttributeValue().withL(new AttributeValue().withBOOL(false), new AttributeValue().withS("nonDefaultValMapValue2"));

        int defaultThreshold = 20;
        Set<String> defaultSet = new HashSet<>();
        Set<String> countryCode = new HashSet<>();
        CaseInsensitiveMap<String, List<Object>> countryCodeValueMap = new CaseInsensitiveMap<>();
        for (int i = 0 ; i < 30 ; i++) {
            countryCode.add("cc"+i);
            if (i < defaultThreshold) {
                countryCodeValueMap.put("cc" + i, defaultVal);
                defaultSet.add("cc" + i);
            } else {
                countryCodeValueMap.put("cc" + i, nonDefaultVal);
            }
        }

        Map<String, AttributeValue> attrValMap = new HashMap<>();
        attrValMap.put("defaultSet", new AttributeValue().withSS(defaultSet));
        attrValMap.put("default", defaultListAttrVal);
        for (int i = 0 ; i < 30 ; i++) {
            if (defaultSet.contains("cc"+i)) {
                continue;
            }
            attrValMap.put("cc"+i, nonDefaultListAttrVal);
        }

        // serialize
        AttributeValue expectedAttrVal = new AttributeValue().withM(attrValMap);
        AttributeValue actualAttrVal = new StringTypeListDefaultMap(countryCodeValueMap, context).getMapAttributeValue(context);
        Assert.assertEquals(expectedAttrVal.getM().size(), actualAttrVal.getM().size());
        for (String key : expectedAttrVal.getM().keySet()) {
            if (expectedAttrVal.getM().get(key).getSS() != null) {
                Assert.assertEquals(new HashSet<>(expectedAttrVal.getM().get(key).getSS()), new HashSet<>(actualAttrVal.getM().get(key).getSS()));
            } else {
                Assert.assertEquals(expectedAttrVal.getM().get(key), actualAttrVal.getM().get(key));
            }
        }

        // deserialize
        Assert.assertEquals(countryCodeValueMap, new StringTypeListDefaultMap(new AttributeValue().withM(attrValMap), context).getValueMap());
    }

    @Test
    public void testDefaultMapWithTemplates() {
        Set<String> defaultSet = new HashSet<>();
        Set<String> countryCode = new HashSet<>();
        CaseInsensitiveMap<String, String> countryCodeValueMap = new CaseInsensitiveMap<>();
        for (int i = 0 ; i < 30 ; i++) {
            countryCode.add("cc"+i);
            countryCodeValueMap.put("cc" + i, "http://host:port/resource/" + "cc" + i+"/queryparams");
            defaultSet.add("cc" + i);
        }

        Map<String, AttributeValue> attrValMap = new HashMap<>();
        attrValMap.put("defaultSet", new AttributeValue().withSS(defaultSet));
        attrValMap.put("default", new AttributeValue().withS("http://host:port/resource{CC}queryparams"));

        // serialize
        Assert.assertEquals(attrValMap.keySet(), new StringStringDefaultMap(TemplateUtils.getTemplateUrlStringStringMap(countryCodeValueMap), context).getMapAttributeValue(context).getM().keySet());
        Assert.assertEquals(attrValMap.get("default").getS(), new StringStringDefaultMap(TemplateUtils.getTemplateUrlStringStringMap(countryCodeValueMap), context).getMapAttributeValue(context).getM().get("default").getS());
        Assert.assertEquals(new HashSet<>(attrValMap.get("defaultSet").getSS()), new HashSet<>(new StringStringDefaultMap(TemplateUtils.getTemplateUrlStringStringMap(countryCodeValueMap), context).getMapAttributeValue(context).getM().get("defaultSet").getSS()));

        // deserialize
        Assert.assertEquals(countryCodeValueMap, TemplateUtils.getResolvedUrlStringStringMapFromTemplateMap(new StringStringDefaultMap(new AttributeValue().withM(attrValMap), context).getValueMap()));
    }
}
