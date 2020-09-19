def cloud_func(data, context):
    from google.cloud import storage
    client = storage.Client()
    bucket = client.get_bucket('mm16b023')
    blob = bucket.get_blob(data['name'])
    x = blob.download_as_string()
    x = x.decode('utf-8')
    lines = x.split('\n')
    lines.remove('')
    print('number of lines : ' + str(len(lines)))
