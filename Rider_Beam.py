import os
import time
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class CastAsDateTime(beam.DoFn):
  def process(self, element):
    wData = element
    timestampFix = wData['timestamp'] / (10 ** 3)
    weatherDate = time.strftime('%Y-%m-%d', time.localtime(timestampFix))
    wData['time_stamp'] = weatherDate
    return wData       
         
PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is required when using the BQ source
options = {
    'project': PROJECT_ID,
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create beam pipeline using local runner
p = beam.Pipeline('DirectRunner', options=opts)

sql = 'SELECT id, time_stamp FROM weather_modeled.weather LIMIT 5'
bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

# write PCollection to log file
query_results | 'Write log 1' >> WriteToText('query_results.txt')

# Par do to teh Lyft count
date_pcoll = query_results | 'Fix Date parDo' >> beam.ParDo(CastAsDateTime())

#write the output table to txt
date_pcoll | 'Write log 5' >> WriteToText('fixed_date_pcoll.txt')

dataset_id = 'weather_modeled'
table_id = 'weather_DF'
schema_id = 'id:STRING,time_stamp:DATE'

# write PCollection to new BQ table
date_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id,
                                                table=table_id,
                                                schema=schema_id,
                                                project=PROJECT_ID,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                batch_size=int(10000000))
result = p.run()
result.wait_until_finish()
