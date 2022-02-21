# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" data_lake_to_mart.py demonstrates a Dataflow pipeline which reads a
large BigQuery Table, joins in another dataset, and writes its contents to a
BigQuery table.
"""


import argparse
from datetime import datetime as dt
import logging
import os
import traceback

import apache_beam as beam
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsDict
from google.cloud import bigquery



class DataLakeToDataMart:
    """A helper class which contains the logic to translate the file into
    a format BigQuery will accept.

    This example uses side inputs to join two datasets together.
    """

    def __init__(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        dir_path = "/".join(dir_path.split("/")[:-1])
        self.schema_str = ''
        # This is the schema of the destination table in BigQuery.
        schema_file = os.path.join(dir_path, 'resources', 'schema_prod.json')
        with open(schema_file) as f:
            data = f.read()
            # Wrapping the schema in fields is required for the BigQuery API.
            self.schema_str = '{"fields": ' + data + '}'

    def get_schema_prod(self, pj):
        """This returns a query. simulate a fact table in a typical
        data warehouse."""
        schema_prod_query = f"""
        SELECT
            *
        FROM
            `{pj}.lake.schema_prod`
        """
        return schema_prod_query


def run(argv=None):
    """The main function which creates the pipeline and runs it."""
    parser = argparse.ArgumentParser()
    # Here we add some specific command line arguments we expect.   S
    # This defaults the output table in your BigQuery you'll have
    # to create the example_data dataset yourself using bq mk temp
    parser.add_argument('--output', dest='output', required=False,
                        help='Output BQ table to write results to.',
                        default='lake.schema_prod')
    parser.add_argument('--pj', dest='pj', required=False,
                    help='ID project',
                    default='pj')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # DataLakeToDataMart is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    data_lake_to_data_mart = DataLakeToDataMart()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))
    schema = parse_table_schema_from_json(data_lake_to_data_mart.schema_str)

    def set_compensation_value(element):
        logging.error("row >>> ", element)
        if 'fecha_de_nacimiento' in element:
            if element['fecha_de_nacimiento']:
                t = dt.strptime(element['fecha_de_nacimiento'], '%Y-%m-%d')
                anios = dt.today().year - t.year

                # Construct a BigQuery client object.
                client = bigquery.Client()

                num_tickets_query = f"""
                    SELECT 
                    cast(count(1) /20 as int64)*3 num_tickets_pref_disp
                    FROM `{known_args.pj}.lake.schema_prod`
                    where cod_vuelo = "{element['cod_vuelo']}";
                """
                logging.error(num_tickets_query)

                num_tickets_dados_query = f"""
                    SELECT 
                    count(1) num_tickets_dados
                    FROM `{known_args.pj}.lake.schema_prod`
                    where cod_vuelo = "{element['cod_vuelo']}" and compensacion in ("ASISTENCIA_PREFERENCIAL", "ASIENTO_PREFERENCIAL");
                """
                logging.error(num_tickets_dados_query)

                query_job_1 = client.query(num_tickets_query).result()  # Make an API request.
                query_job_2 = client.query(num_tickets_dados_query).result()  # Make an API request.

                logging.error(query_job_1)
                logging.error(query_job_2)

                compensacion = "NO APLICA"
                for row in query_job_1:
                    a = row["num_tickets_pref_disp"]
                for row in query_job_2:
                    b = row["num_tickets_dados"]
                
                if int(a) - int(b) > 0:
                    if anios < 14:
                        compensacion = "ASISTENCIA_PREFERENCIAL"
                    elif anios > 60:
                        compensacion = "ASIENTO_PREFERENCIAL"
                    
                    update_query = f"""
                    update `{known_args.pj}.lake.schema_prod`
                    set compensacion = "{compensacion}"
                    FROM `{known_args.pj}.lake.schema_prod`
                    where cod_vuelo = "{element['cod_vuelo']}" and dni = "{element['dni']}";
                    """
                    logging.error(update_query)
                    query_job_3 = client.query(update_query)  # Make an API request.
            else:
                pass
        return element


    schema_prod_query = data_lake_to_data_mart.get_schema_prod(known_args.pj)
    (
        p
        # Read the orders from BigQuery.  This is the source of the pipeline.  All further
        # processing starts with rows read from the query results here.
        | 'Read Ventas from BigQuery ' >> beam.io.Read(
            beam.io.BigQuerySource(query=schema_prod_query, use_standard_sql=True))
        | 'Set compensation' >> beam.Map(lambda s: set_compensation_value(s))
        # This is the final stage of the pipeline, where we define the destination
        # of the data.  In this case we are writing to BigQuery.
    )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
