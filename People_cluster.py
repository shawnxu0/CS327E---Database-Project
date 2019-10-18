import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


# PTransform: parse line in file, splits up every entry in the profession column into 3 different columns. Returns a list that can be iterated on
class SplitProfessionFn(beam.DoFn):
    def process(self, element):
        record = element
        nconst = record.get('nconst')
        primaryProfession = record.get('primaryProfession')
        if primaryProfession == None:
            return [[nconst]]
        splitProf = primaryProfession.split(',')
        if len(splitProf) == 1:
            return [[nconst, splitProf[0]]]
        if len(splitProf) == 2:
            return [[nconst, splitProf[0], splitProf[1]]]
        if len(splitProf) == 3:
            return [[nconst, splitProf[0], splitProf[1], splitProf[2]]]

# PTransform: creates BQ records. Takes the given list and creates a new record with columns nconst, profession1, profession2, and profession3
class MakeRecordFn(beam.DoFn):
    def process(self, element):
        if len(element) == 1:
            nconst = element
            record = {'nconst': nconst, 'profession1': None, 'profession2': None, 'profession3': None}
            return [record]
        if len(element) == 2:
            nconst, profession1 = element
            record = {'nconst': nconst, 'profession1': profession1, 'profession2': None, 'profession3': None}
            return [record]
        if len(element) == 3:
            nconst, profession1, profession2 = element
            record = {'nconst': nconst, 'profession1': profession1, 'profession2': profession2, 'profession3': None}
            return [record]
        if len(element) == 4:
            nconst, profession1, profession2, profession3 = element
            record = {'nconst': nconst, 'profession1': profession1, 'profession2': profession2, 'profession3': profession3}
            return [record]


PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID,
	'runner': 'DataFlowRunner',
    'staging_location': 'gs://shawnrussell2019-imdb/staging',
    'temp_location': 'gs://shawnrussell2019-imdb/temp'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution
with beam.Pipeline('DataFlowRunner', options=opts) as p:
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT nconst, primaryProfession FROM imdb_modeled.People'))
	
    # write PCollection to log file
    query_results | 'Write to input' >> WriteToText('inputProf.txt')

    # apply ParDo to the PCollection
    split_pcoll = query_results | 'Split Professions' >> beam.ParDo(SplitProfessionFn())

    # make BQ records
    results_pcoll = split_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
	
	# write PCollection to log file
    results_pcoll | 'Write to output' >> WriteToText('outputProf.txt')

    qualified_table_name = PROJECT_ID + ':imdb_modeled.primaryProfessions_Beam_DF'
    table_schema = 'nconst:STRING,profession1:STRING,profession2:STRING,profession3:STRING'
	
	#write to bigquery
    results_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                                          schema=table_schema,
                                                                          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                          write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))