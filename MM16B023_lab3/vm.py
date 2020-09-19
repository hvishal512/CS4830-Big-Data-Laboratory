from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('mm16b023')
blob = bucket.get_blob( 'addresses.csv' )
x = blob.download_as_string()
x = x.decode( 'utf-8' )
lines = x.split('\n')
lines.remove('')
print('Number of lines are : ',str(len(lines)))
