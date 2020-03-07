import keyvalue.sqlitekeyvalue as KeyValue
import keyvalue.parsetriples as ParseTriples
import keyvalue.stemmer as Stemmer

# Make connections to KeyValue
kv_labels = KeyValue.SqliteKeyValue("./SQLDB/sqlite_labels.db","labels",sortKey=True)
kv_images = KeyValue.SqliteKeyValue("./SQLDB/sqlite_images.db","images")

# Process Logic.
parse_images = ParseTriples.ParseTriples('../Dataset/images.ttl')
parse_labels = ParseTriples.ParseTriples('../Dataset/labels_en.ttl')

#Images 
for i in range(100000):
    line = parse_images.getNext()
    category = line[0]
    b = line[1]
    imageURL = line[2]
    if b == 'http://xmlns.com/foaf/0.1/depiction':
        kv_images.put(category, imageURL)

#Labels
for i in range(100000):
    line = parse_labels.getNext()
    category = line[0]
    b = line[1]
    terms = line[2]
    image = kv_images.get(category)
    if b == 'http://www.w3.org/2000/01/rdf-schema#label' and image is not None:
        for token in terms.split(' '):
            stemmedWord = Stemmer.stem(token)
            kv_labels.putSort(stemmedWord, str(i), category)
    else:
        i = i - 1

# Close KeyValues Storages
kv_labels.close()
kv_images.close()
