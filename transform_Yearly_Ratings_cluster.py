from __future__ import absolute_import
import apache_beam as beam
import os, datetime
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# extracting song titles
class ExtractFn(beam.DoFn):

    def process(self, element):
        name = element
        title = name.get('Title')
        return [(title,1)]
     
# summing song appearances on Billboard charts
class SumBillboardFn(beam.DoFn):
  
    def process(self, element):
        name, counts_obj = element
        counts = list(counts_obj)
        sum_counts = len(counts)
        return [(name, sum_counts)]  

# creating BQ record
class MakeBQRecordFn(beam.DoFn):
  
    def process(self, element):
        name, total_appearances = element
        record = {'Title' : name, 'Years_on_chart' : total_appearances} 
        return [record]   
    


PROJECT_ID = 'coherent-server-252621'
BUCKET = 'gs://shawnrussell2019-imdb'
DIR_PATH_IN = BUCKET + '/input/' 
DIR_PATH_OUT = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'runner': 'DataflowRunner',
    'job_name': 'year-cluster',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DataflowRunner', options=opts) as p:

    # create a PCollection from the file contents.
    in_pcoll = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT Title from billboard_modeled.Yearly_Ratings'))

    # write PCollection to log file
    in_pcoll | 'Write to input' >> WriteToText('input.txt')
    
    # apply a ParDo to the PCollection 
    extract_pcoll = in_pcoll | 'Extract songs' >> beam.ParDo(ExtractFn())

    # write PCollections to files
    extract_pcoll | 'Write to extract' >> WriteToText('extract.txt')

    # apply GroupByKey 
    grouped_pcoll = extract_pcoll | 'Group by song' >> beam.GroupByKey()
    
    # write PCollections to files
    grouped_pcoll | 'Write to grouped' >> WriteToText('grouped.txt')

    # Sum number of appearances in Billboard charts
    summed_pcoll = grouped_pcoll | 'Sum up Billboard appearances' >> beam.ParDo(SumBillboardFn())
    
    # write PCollections to files
    summed_pcoll | 'Write to summed' >> WriteToText('summed.txt')
    
    # make BQ records
    bq_summed_pcoll = summed_pcoll | 'Make BQ Record' >> beam.ParDo(MakeBQRecordFn())
    
    # write BQ records to files
    bq_summed_pcoll | 'Write BQ records' >> WriteToText('bq_records.txt')
    
    title_table_name = PROJECT_ID + ':Yearly_Ratings_workflow_modeled.Years_Charting'
    table_schema = 'Title:STRING,Years_on_chart:INTEGER'
        
    # write Pcoll to BQ tables
    bq_summed_pcoll | 'Write Years Charting table' >> beam.io.Write(beam.io.BigQuerySink(title_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))