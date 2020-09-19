import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
options = PipelineOptions()
google_cloud_options = options .view_as(GoogleCloudOptions)
google_cloud_options.project = 'hopeful-buckeye-266720' #Enter your project ID
google_cloud_options.job_name = 'bdl3'
google_cloud_options.temp_location = "gs://mm16b023/tmp" # This is to store temp
#results, format is gs: //my_bucket/tmp
options.view_as(StandardOptions).runner = 'DataflowRunner'
p = beam.Pipeline( options = options )
lines = p | 'Read' >> beam.io.ReadFromText( 'gs://iitm/files/file.txt' ) |'counting lines' >> beam.combiners.Count.Globally(sum) | 'Write' >> beam.io.WriteToText( 'gs://mm16b023/output_beam/')      
result = p.run()

