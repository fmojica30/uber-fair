import os, datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class LyftFn(beam.DoFn):
  def process(self, element):
    values = element
    ID = values.get('id')
    TIME_STAMP = values.get('time_stamp')
    PRICE = values.get('price')
    CAB_TYPE = values.get('cab_type')
    PRODUCT_ID = values.get('product_id')
    if 'LYFT' in CAB_TYPE or 'Lyft' in CAB_TYPE:
            return {'id':ID, 'cab_type':CAB_TYPE}

class UberFn(beam.DoFn):
  def process(self, element):
    values = element
    ID = values.get('id')
    TIME_STAMP = values.get('time_stamp')
    PRICE = values.get('price')
    CAB_TYPE = values.get('cab_type')
    PRODUCT_ID = values.get('product_id')
    if 'UBER' in CAB_TYPE or 'Uber' in CAB_TYPE:
            return {'id':ID, 'cab_type':CAB_TYPE}

class FixFormatFn(beam.DoFn):
  def process(self, element):
    tuple = element
    id, CAB_TYPE = tuple[0],tuple[1]
    return [{'id':id, 'cab_type':CAB_TYPE}]


PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# run pipeline on Dataflow
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-rider',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-4', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
    'num_workers': 5
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

p = beam.Pipeline('DataflowRunner', options=opts)
 
sql = 'SELECT * FROM rideshare_modeled.Rider'
bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

# write PCollection to log file
query_results | 'Write log 1' >> WriteToText(DIR_PATH + 'query_results.txt')

# Par do to the Lyft count
lyft_pcoll = query_results | 'Lyft Count Fn' >> beam.ParDo(LyftFn())

#checking the pcoll
lyft_pcoll | 'write log 2' >> WriteToText(DIR_PATH + 'lyft_pcoll.txt')

# ParDo to the Uber count
uber_pcoll = query_results | 'Uber Count Fn' >> beam.ParDo(UberFn())

#checking the pcoll
uber_pcoll | 'write log 3' >> WriteToText(DIR_PATH + 'uber_pcoll.txt')

# Combine both query results into one
all_rides_pcoll = (lyft_pcoll, uber_pcoll)| 'Merge pCollections' >> beam.Flatten()

# write all rides to output
all_rides_pcoll | 'Write log 4' >> WriteToText(DIR_PATH + 'all_rides_pcoll.txt')

# fix the format of the output table
fixed_rides_pcoll = all_rides_pcoll | 'Fix Format' >> beam.ParDo(FixFormatFn())

#write the output table to txt
fixed_rides_pcoll | 'Write log 5' >> WriteToText(DIR_PATH + 'fixed_rides_pcoll.txt')

dataset_id = 'rideshare_modeled'
table_id = 'Rider_Beam_DF'
schema_id = 'id:STRING, cab_type:STRING'

# write PCollection to new BQ table
fixed_rides_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id,
                                                table=table_id,
                                                schema=schema_id,
                                                project=PROJECT_ID,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                batch_size=int(100))
result = p.run()
result.wait_until_finish()
