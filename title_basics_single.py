import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


# PTransform: parse line in file, splits up every entry in the genre column into 3 different columns. Returns a list that can be iterated on
class SplitGenreFn(beam.DoFn):
    def process(self, element):
        record = element
        tconst = record.get('tconst')
        genres = record.get('genres')
        if genres == None:
            return [[tconst]]
        splitgenres = genres.split(',')
        if len(splitgenres) == 1:
            return [[tconst, splitgenres[0]]]
        if len(splitgenres) == 2:
            return [[tconst, splitgenres[0], splitgenres[1]]]
        if len(splitgenres) == 3:
            return [[tconst, splitgenres[0], splitgenres[1], splitgenres[2]]]

# PTransform: creates BQ records. Takes the given list and creates a new record with columns tconst, genre1, genre2, and genre 3
class MakeRecordFn(beam.DoFn):
    def process(self, element):
        if len(element) == 1:
            tconst = element
            record = {'tconst': tconst, 'genre1': None, 'genre2': None, 'genre3': None}
            return [record]
        if len(element) == 2:
            tconst, genre1 = element
            record = {'tconst': tconst, 'genre1': genre1, 'genre2': None, 'genre3': None}
            return [record]
        if len(element) == 3:
            tconst, genre1, genre2 = element
            record = {'tconst': tconst, 'genre1': genre1, 'genre2': genre2, 'genre3': None}
            return [record]
        if len(element) == 4:
            tconst, genre1, genre2, genre3 = element
            record = {'tconst': tconst, 'genre1': genre1, 'genre2': genre2, 'genre3': genre3}
            return [record]


PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID,
	'runner': 'DirectRunner',
    'staging_location': 'gs://shawnrussell2019-imdb/staging',
    'temp_location': 'gs://shawnrussell2019-imdb/temp'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution
with beam.Pipeline('DirectRunner', options=opts) as p:
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT tconst,genres FROM imdb_modeled.title_basics LIMIT 500'))
	
    # write PCollection to log file
    query_results | 'Write to input' >> WriteToText('input.txt')

    # apply ParDo to the PCollection
    genres_pcoll = query_results | 'Split Genres' >> beam.ParDo(SplitGenreFn())

    # make BQ records
    results_pcoll = genres_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
	
	# write PCollection to log file
    results_pcoll | 'Write to output' >> WriteToText('output.txt')

    qualified_table_name = PROJECT_ID + ':imdb_modeled.Genres_New_title_basics'
    table_schema = 'tconst:STRING,genre1:STRING,genre2:STRING,genre3:STRING'
	
	#write to bigquery
    results_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                                          schema=table_schema,
                                                                          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                          write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))