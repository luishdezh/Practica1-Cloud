import keyvalue.sqlitekeyvalue as KeyValue
import keyvalue.parsetriples as ParseTripe
import keyvalue.stemmer as Stemmer
import sys

# Make connections to KeyValue
kv_labels = KeyValue.SqliteKeyValue("./SQLDB/sqlite_labels.db","labels",sortKey=True)
kv_images = KeyValue.SqliteKeyValue("./SQLDB/sqlite_images.db","images")

# Process Logic.
arg = sys.argv

for i in range(1, len(arg)):
    a = arg[i]
    stemmedWord = Stemmer.stem(a)
    category = kv_labels.get(stemmedWord)
    if category is None:
        print("No se encontraron imágenes con " + a)
    else:
        image = kv_images.get(category)
        print("Imagen: "+ image + "\n")

# Close KeyValues Storages
kv_labels.close()
kv_images.close()

