""" data_ingest.py is a Dataflow pipeline which reads a file and writes
its contents to a BigQuery table.

This example reads a json schema of the intended output into BigQuery,
"""


import argparse
import csv
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json


class DataTransformation:
    """A helper class which contains the logic to translate the file into a
  format BigQuery will accept."""

    def __init__(self, schema_file_name):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        dir_path = "/".join(dir_path.split("/")[:-1])
        self.schema_str = ''
        # Here we read the output schema from a json file.  This is used to specify the types
        # of data we are writing to BigQuery.
        schema_file = os.path.join(dir_path, 'resources', schema_file_name)
        with open(schema_file, 'r') as f:
            data = f.read()
            # Wrapping the schema in fields is required for the BigQuery API.
            self.schema_str = '{"fields": ' + data + '}'
            f.close()

    def parse_method(self, string_input):
        """This method translates a single line of comma separated values to a
    dictionary which can be loaded into BigQuery.

        Args:
            string_input: A comma separated list of values

        Returns:
            A dict mapping BigQuery column names as keys to the corresponding value
            parsed from string_input.  In this example, the data is not transformed, and
            remains in the same format as the CSV.  There are no date format transformations.
        """
        # Strip out return characters and quote characters.
        schema = parse_table_schema_from_json(self.schema_str)

        field_map = [f for f in schema.fields]

        # Use a CSV Reader which can handle quoted strings etc.
        reader = csv.reader(string_input.split('\n'))
        for csv_row in reader:
            csv_row_trx = "".join([x for x in csv_row])
            csv_row_trx = csv_row_trx.split("|")
            row = {}
            i = 0
            # Iterate over the values from our csv file, applying any transformation logic.
            for value in csv_row_trx:
                # If the schema indicates this field is a date format, we must
                # transform the date from the source data into a format that
                # BigQuery can understand.
                if field_map[i].type == 'DATE':
                    # Format the date to YYYY-MM-DD format which BigQuery
                    # accepts.
                    value = csv_row_trx[i].replace("/", "-")
                elif field_map[i].type == 'TIMESTAMP':
                    # Format the date to YYYY-MM-DD hh:mm:ss format which BigQuery
                    # accepts.
                    value = f"{csv_row_trx[i][0:4]}-{csv_row_trx[i][4:6]}-{csv_row_trx[i][6:8]}{csv_row_trx[i][8:]}"

                row[field_map[i].name] = value
                i += 1

            return row


def run(argv=None):
    """The main function which creates the pipeline and runs it."""
    parser = argparse.ArgumentParser()
    # Here we add some specific command line arguments we expect.   Specifically
    # we have the input file to load and the output table to write to.
    parser.add_argument(
        '--input', dest='input', required=False,
        help='Input file to read.  This can be a local file or a file in a Google Storage Bucket.',
        default='gs://<PROJECT>/data_files/pasajero.csv,gs://<PROJECT>/data_files/vuelo.csv,gs://<PROJECT>/data_files/venta.csv')
    # This defaults to the temp dataset in your BigQuery project.  You'll have
    # to create the temp dataset yourself using bq mk temp
    parser.add_argument('--output', dest='output', required=False,
                        help='Output BQ table to write results to.',
                        default='lake.pasajero,lake.vuelo,lake.venta')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line.  This includes information like where Dataflow should
    # store temp files, and what the project id is.
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    pasajero_trx = DataTransformation("pasajero.json")
    schema_pasajero = parse_table_schema_from_json(pasajero_trx.schema_str)
    vuelo_trx = DataTransformation("vuelo.json")
    schema_vuelo = parse_table_schema_from_json(vuelo_trx.schema_str)
    venta_trx = DataTransformation("venta.json")
    schema_venta = parse_table_schema_from_json(venta_trx.schema_str)

    (p
    # Read the file.  This is the source of the pipeline.  All further
    # processing starts with lines read from the file.  We use the input
    # argument from the command line.  We also skip the first line which is a
    # header row.
    | 'Pasajero Read From Text' >> beam.io.ReadFromText(known_args.input.split(",")[0])
    # This stage of the pipeline translates from a CSV file single row
    # input as a string, to a dictionary object consumable by BigQuery.
    # It refers to a function we have written.  This function will
    # be run in parallel on different workers using input from the
    # previous stage of the pipeline.
    | 'Pasajero Trx to BigQuery Row' >> beam.Map(lambda s: pasajero_trx.parse_method(s))
    | 'Psajero Write to BigQuery' >> beam.io.Write(
        beam.io.BigQuerySink(
            # The table name is a required argument for the BigQuery sink.
            # In this case we use the value passed in from the command line.
            known_args.output.split(",")[0],
            # Here we use the JSON schema read in from a JSON file.
            # Specifying the schema allows the API to create the table correctly if it does not yet exist.
            schema=schema_pasajero,
            # Creates the table in BigQuery if it does not yet exist.
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # Deletes all data in the BigQuery table before writing.
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    )
    (p
    # Read the file.  This is the source of the pipeline.  All further
    # processing starts with lines read from the file.  We use the input
    # argument from the command line.  We also skip the first line which is a
    # header row.
    | 'Vuelo Read From Text' >> beam.io.ReadFromText(known_args.input.split(",")[1])
    # This stage of the pipeline translates from a CSV file single row
    # input as a string, to a dictionary object consumable by BigQuery.
    # It refers to a function we have written.  This function will
    # be run in parallel on different workers using input from the
    # previous stage of the pipeline.
    | 'Vuelo Trx to BigQuery Row' >> beam.Map(lambda s: vuelo_trx.parse_method(s))
    | 'Vuelo Write to BigQuery' >> beam.io.Write(
        beam.io.BigQuerySink(
            # The table name is a required argument for the BigQuery sink.
            # In this case we use the value passed in from the command line.
            known_args.output.split(",")[1],
            # Here we use the JSON schema read in from a JSON file.
            # Specifying the schema allows the API to create the table correctly if it does not yet exist.
            schema=schema_vuelo,
            # Creates the table in BigQuery if it does not yet exist.
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # Deletes all data in the BigQuery table before writing.
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    )
    (p
    # Read the file.  This is the source of the pipeline.  All further
    # processing starts with lines read from the file.  We use the input
    # argument from the command line.  We also skip the first line which is a
    # header row.
    | 'Venta Read From Text' >> beam.io.ReadFromText(known_args.input.split(",")[2])
    # This stage of the pipeline translates from a CSV file single row
    # input as a string, to a dictionary object consumable by BigQuery.
    # It refers to a function we have written.  This function will
    # be run in parallel on different workers using input from the
    # previous stage of the pipeline.
    | 'Venta Trx to BigQuery Row' >> beam.Map(lambda s: venta_trx.parse_method(s))
    | 'Venta Write to BigQuery' >> beam.io.Write(
        beam.io.BigQuerySink(
            # The table name is a required argument for the BigQuery sink.
            # In this case we use the value passed in from the command line.
            known_args.output.split(",")[2],
            # Here we use the JSON schema read in from a JSON file.
            # Specifying the schema allows the API to create the table correctly if it does not yet exist.
            schema=schema_venta,
            # Creates the table in BigQuery if it does not yet exist.
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # Deletes all data in the BigQuery table before writing.
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    )
    p.run().wait_until_finish()
        

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
