import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
# PTransform: parse line in file, return (Lyft, 1)
class LyftCountFn(beam.DoFn):
  def process(self, element):
    values = element
    ID = values.get('id')
    TIME_STAMP = values.get('time_stamp')
    PRICE = values.get('price')
    CAB_TYPE = values.get('cab_type')
    PRODUCT_ID = values.get('product_id')
    if 'LYFT' in CAB_TYPE or 'Lyft' in CAB_TYPE:
            return [(ID, CAB_TYPE)]
class MakeRecordFn(beam.DoFn):
  def process(self, element):
     id, cab_type = element
     record = {'id': id, 'cab_type': cab_type}
     return [record]
PROJECT_ID = os.environ['PROJECT_ID']
# Project ID is needed for BigQuery data source, even for local execution.
options = { 
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)
# Create a Pipeline using a local runner for execution
with beam.Pipeline('DirectRunner', options=opts) as p:
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='S
ELECT * FROM rideshare_modeled.Rider'))
    # write PCollection to log file
    query_results | 'Write to input' >> WriteToText('input.txt')
    # apply ParDo to the PCollection
    in_pcoll = query_results | 'Extract Number of Lyft rides' >> beam.ParDo(LyftCountFn())
    # write PCollection to log file
    in_pcoll | 'Write to output' >> WriteToText('output.txt')
    #making the big query records
    out_pcoll = in_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
    qualified_table_name = PROJECT_ID + ':rideshare_modeled.beam_pardo'
    table_schema = 'id:STRING,cab_type:STRING'
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
