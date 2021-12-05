package com.theresonancelabs.resonance.util;

import org.apache.commons.collections4.map.CaseInsensitiveMap;

public class TemplateUtils {
    public static CaseInsensitiveMap<String, String> getTemplateUrlStringStringMap(CaseInsensitiveMap<String, String> urlMap) {
        if (urlMap == null || urlMap.size() == 0) {
            return urlMap;
        }

        CaseInsensitiveMap<String, String> templateUrlMap = new CaseInsensitiveMap<>();
        for(String key : urlMap.keySet()) {
            String templateUrl = urlMap.get(key).replace("/"+key+"/", "{CC}");
            templateUrlMap.put(key, templateUrl);
        }
        return templateUrlMap;
    }

    public static CaseInsensitiveMap<String, String> getResolvedUrlStringStringMapFromTemplateMap(CaseInsensitiveMap<String, String> templateUrlMap) {
        if (templateUrlMap == null || templateUrlMap.size() == 0) {
            return templateUrlMap;
        }

        CaseInsensitiveMap<String, String> urlMap = new CaseInsensitiveMap<>();
        for(String key : templateUrlMap.keySet()) {
            String resolvedUrl= templateUrlMap.get(key).replace("{CC}", "/"+key+"/");
            urlMap.put(key, resolvedUrl);
        }
        return urlMap;
    }
}
