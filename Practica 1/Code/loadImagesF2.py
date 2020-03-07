import keyvalue.parsetriples as ParseTriples
import keyvalue.stemmer as Stemmer
import keyvalue.dynamostorage as Dynamo
from botocore.exceptions import ClientError

kv_labels = Dynamo.DynamodbKeyValue('labels')
kv_images = Dynamo.DynamodbKeyValue('images')

# Process Logic.
parse_images = ParseTriples.ParseTriples('../Dataset/images.ttl')
parse_labels = ParseTriples.ParseTriples('../Dataset/labels_en.ttl')

# Insert Images 
for i in range(2000):
    line = parse_images.getNext()
    category = line[0]
    b = line[1]
    imageURL = line[2]
    if b == 'http://xmlns.com/foaf/0.1/depiction':
        kv_images.put(category, len(category), imageURL)
    else:
        i = i -1

#Labels
for i in range(5000):
    line = parse_labels.getNext()
    category = line[0]
    b = line[1]
    terms = line[2]
    findImage = kv_images.get(category, len(category))
    if b == 'http://www.w3.org/2000/01/rdf-schema#label' and findImage is not None:
        for token in terms.split(' '):
            stemmedWord = Stemmer.stem(token)
            kv_labels.put(stemmedWord, len(stemmedWord), category)
    else:
        i = i - 1
