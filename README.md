# JSON Text Compactness  for Database Storage 

Today, we'd like to share a simple innovation in how we structure the JSON documents that are stored in the database - we coalesce the redundant / repeated information in the JSON doc to come up with a compact JSON document that is reduced in size without losing any textual readability in the JSON doc.  Our compactness technique is somewhere between the [JSON Compression techniques](https://www.lucidchart.com/techblog/2019/12/06/json-compression-alternative-binary-formats-and-compression-methods/) and [JSON Normalization techniques](https://pandas.pydata.org/pandas-docs/version/1.2.0/reference/api/pandas.json_normalize.html) (JSON Compression focuses on serializing json for storage and loses text readability and JSON Normalization techniques normalize the JSON into flat dataframe structures which is somewhat different from what we are trying to achieve)

Though, the redundancy and repeatedness of the information that we've solved for is unique to our usecase, we do believe similar patterns can be used in different situations to compact JSON documents by identifying redundancy and repeatedness in these usecases as well. 

We've used AWS Dynamo DB as our database - it has worked well for the usecases that we've had and made development a breeze.  We've also experimented the same JSON document storage on the Azure Cosmos DB which also has worked like a charm. 

So, without further ado, lets setup our example and see how we compact the JSON document.

## Usecase

Lets say we have a Songs table in the database that stores the details about the different Songs that the users have played / are available in the library.

Our app is available in multiple marketplaces (US, Canada, UK etc) and our JSON document stores the song details for each of these marketplaces in the same JSON document (simplifies cross region song sharing usecases - a consumer in US shares a song with a friend in the UK). 

From experience, we've observed that for a large majority of the songs, the song attributes do not differ between the different marketplaces. In some cases the song attributes do differ in different marketplaces. Lets suppose we have the following Song JSON document with a single albumID attribute that is a string type and we have 7 marketplaces. In the example below, marketplace1-4  have same album Id (AlbumID1) and marketplace5-7 have a different  album Id (AlbumID2).

```
{
	"songID":  <system generated PartitionKey unique across all marketplaces> 
	"albumID": 
	{
		"<marketplace1>":  "<albumID1>",
		"<marketplace2>":  "<albumID1>",
		"<marketplace3>":  "<albumID1>",
		"<marketplace4>":  "<albumID1>",
  
		"<marketplace5>":  "<albumID2>", 
		"<marketplace6>":  "<albumID2>",
		"<marketplace7>":  "<albumID2>"
	}
}
```

We compact the Song JSON document's albumID attribute by defining a **defaultSet** that is the largest set of marketplaces with the same value and then save the **defaultSet** and **defaultValue** for these marketplaces. The marketplaces that are are different are kept as is. The json doc with the compacted albumID attribute would look like the following:

```
{
	"songID":  <system generated PartitionKey unique across all marketplaces> 
	"albumID": 
	{
		"defaultSet": 
		[ 
			"<marketplace1>", "<marketplace2>", "<marketplace3>", "<marketplace4>"
		],
		"defaultValue": "<albumID1>",
		"<marketplace5>":  "<albumID2>", 
		"<marketplace6>":  "<albumID2>",
		"<marketplace7>":  "<albumID2>"
	}
}
```
 
There is an additional optimization that could be made - defining ranked default sets with the repeat frequency - but we did not feel the need to implement these. 

```
{
	"songID":  <system generated PartitionKey unique across all marketplaces> 
	"albumID": 
	{
		"defaultSet1": 
		[ 
			"<marketplace1>", "<marketplace2>", "<marketplace3>", "<marketplace4>"
		],
		"defaultValue1": "<albumID1>",
		"defaultSet2": 
		[ 
			"<marketplace5>", "<marketplace6>", "<marketplace7>"
		],
		"defaultValue2": "<albumID2>"
	}
}
```

## Performance

Our app is available in 32 marketplaces and each song has approximately 16 attributes that are to be duplicated for each marketplace, the average document sizes for each song JSON document came out to ~ 405KB (song contains a bunch of song metadata specific to each marketplace etc). With the JSON Text Compactness algo, we reduced the JSON doc size to ~64 KB without any noticeable impact in serialization / deserialization performance.

## Implementation

In terms of implementation, we defined a java class with the following interface (this is for storing String value type):

```
class StringStringDefaultMap 
{     

	// Serialize constructor that serializes a <marketplace, albumId> map to a compact representation
	public StringStringDefaultMap(CaseInsensitiveMap<String, String> valueMap, Context context);

	// Deserialize constructor that deserializes a dynamo db attribute value compacted map to a <marketplace, albumId> map
	public StringStringDefaultMap(AttributeValue stringAttributeValueMapAttrVal, Context context);

	// Get an attribute value for the compacted map represented by this class - it will create the JSON document per the DynamoDB JSON format where type is encoded into the json
	public AttributeValue getMapAttributeValue(Context context);  

	// Get the <marketplaceId, albumId> value map represented by this class
	public CaseInsensitiveMap<String, String> getValueMap();

	// Get the default <marketplace, albumId> key value pairs - albumId should be the same for all the marketplaces in this map since its the default map
	public CaseInsensitiveMap<String, String> getDefaultMap()

	// Get the default keyset for the compacted map
	public Set getDefaultKeySet();
}
```

The implementation for the compacted attributes for AWS Dynamo DB encodes the type into the JSON document as required by the DynamoDB JSON format. This implementation also reuses the Dynamo DB String Set type to store the defaultSet. here is what the above example would look like: 

```
{
  "songID": {
    "S": "<system generated PartitionKey unique across all marketplaces> "
	}
  "albumID": 
  {
    "M": {
      "defaultSet1": {
        "SS": 
        [ 
          "<marketplace1>", "<marketplace2>", "<marketplace3>", "<marketplace4>"
        ],
      }
      "defaultValue1": {
        "S": "<albumID1>",
      }
      "<marketplace5>":  {
        "S": "<albumID2>", 
      }
      "<marketplace6>":  {
        "S": "<albumID2>",
      }
      "<marketplace7>":  {
        "S": "<albumID2>"
      }
    }
}
```

In this example, the album ID value type String so we used a StringStringDefaultMap. There are also implementations for the different types that DynamoDB supports such as 

```
	<String marketplace, Integer attribute value> // Dynamo DB's number attribute type
	<String marketplace, List<Map<String, Object>>> // Dynamo DB's List Attribute Type where each list element is map attribute type
	<String marketplace, List<String>> // List<String> attribute type
	<String marketplace, List<Object>> // List<Object> attribute type
	<String marketplace, Set<String>> // String Set attribute type
	<String marketplace, Map<String,String>> // Map<String, String> attribute type
```

Another trick we had to implement is to replace marketplace specific references in an attribute value with a placeholder for storage in the database and resolve these at deserialization time. For example, the following data urls were coalesced to a placeholder data url:

```
https://www.letsresonate.net/us/albums?id=<albumId1> 
https://www.letsresonate.net/ca/albums?id=<albumId1> 
https://www.letsresonate.net/uk/albums?id=<albumId1> 
=>
https://www.letsresonate.net/{PLACE_HOLDER}/albums?id=<albumId1> 
```
## Usage
The code is shared in the repository: https://github.com/resonancedeveloper/JSONTextCompactness

The Tests in the TestDefaultMaps demonstrate on how to use each of the default map classes and serialize / deserialize into dynamo db json. Here is one such example:

```
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
```