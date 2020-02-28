import os, datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
import time
           
class CastAsDateTime(beam.DoFn):
  def process(self, element):
    wData = element
    print(wData["time_stamp"])
    weatherDate = str(time.strftime('%Y-%m-%d', time.localtime(wData["time_stamp"])))
    wData['time_stamp'] = weatherDate
    print(wData)
    return [wData]

  
         
PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# run pipeline on Dataflow 
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-student',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-4', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
    'num_workers': 1
}

opts = beam.pipeline.PipelineOptions(flags=[], **options)

p = beam.Pipeline('DataflowRunner', options=opts)

sql = 'SELECT id, time_stamp FROM weather_modeled.weather'
bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

# write PCollection to log file
query_results | 'Write log 1' >> WriteToText(DIR_PATH + 'query_results.txt')

# apply ParDo to format the student's date of birth  
formatted_date_pcoll = query_results | 'Format DOB' >> beam.ParDo(CastAsDateTime())

# write PCollection to log file
formatted_date_pcoll | 'Write log 2' >> WriteToText(DIR_PATH + 'formatted_date_pcoll.txt')


dataset_id = 'weather_modeled'
table_id = 'weather_beam_df'
schema_id = 'id:STRING, time_stamp:DATE'

# write PCollection to new BQ table
formatted_date_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                table=table_id, 
                                                schema=schema_id,
                                                project=PROJECT_ID,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                batch_size=int(100))
result = p.run()
result.wait_until_finish()