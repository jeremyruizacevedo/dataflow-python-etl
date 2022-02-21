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

    def get_ventas_query(self, pj):
        """This returns a query. simulate a fact table in a typical
        data warehouse."""
        ventas_query = f"""
        SELECT
            codigo_aerolinea,
            cod_avion,
            asiento,
            dni,
            monto,
            estado,
            fecha_reserva,
            fecha_compra,
            categoria
        FROM
            `{pj}.lake.venta` venta
        """
        return ventas_query

    def add_pasajero(self, row, pasajero):
        """add_pasajero joins two datasets together.  Dataflow passes in the
        a row from the orders dataset along with the entire account details dataset.

        This works because the entire account details dataset can be passed in memory.

        The function then looks up the account details, and adds all columns to a result
        dictionary, which will be written to BigQuery."""
        result = row.copy()
        try:
            result.update(pasajero[row['dni']])
        except KeyError as err:
            traceback.print_exc()
            logging.error("pasajero Not Found error: %s", err)
        return result

    def add_vuelo(self, row, vuelo):
        """add_vuelo joins two datasets together.  Dataflow passes in the
        a row from the orders dataset along with the entire account details dataset.

        This works because the entire account details dataset can be passed in memory.

        The function then looks up the account details, and adds all columns to a result
        dictionary, which will be written to BigQuery."""
        result = row.copy()
        try:
            result.update(vuelo[row['cod_avion']])
        except KeyError as err:
            traceback.print_exc()
            logging.error("Vuelo Not Found error: %s", err)
        return result


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

    def get_absolute_value_amount(element):
        if 'monto' in element:
            element["monto"] = abs(element["monto"])
        return element
    
    def obfuscate_data(element):
        if 'nombre_completo' in element:
            name = element["nombre_completo"].split(" ")[0]
            last_name = " ".join(element["nombre_completo"].split(" ")[1:])
            obfuscated_last_name = ""
            for x in last_name:
                if x not in (last_name[0], " "):
                    obfuscated_last_name += "*"
                else:
                    obfuscated_last_name += x
            element["nombre_completo"] = name + " " + obfuscated_last_name
        return element

    # This query returns details about the account, normalized into a
    # different table.  We will be joining the data in to the main orders dataset in order
    # to create a denormalized table.
    pasajero_source = (
        p
        | 'Read Pasajero from BigQuery ' >> beam.io.Read(
            beam.io.BigQuerySource(query=f"""
                SELECT
                  *
                FROM
                  `{known_args.pj}.lake.pasajero`""",
                                   # This next stage of the pipeline maps the acct_number to a single row of
                                   # results from BigQuery.  Mapping this way helps Dataflow move your data around
                                   # to different workers.  When later stages of the pipeline run, all results from
                                   # a given account number will run on one worker.
                                   use_standard_sql=True))
        | 'Pasajero Details' >> beam.Map(
            lambda row: (
                row['dni'], row
            )))
    
    vuelo_source = (
        p
        | 'Read Vuelo from BigQuery ' >> beam.io.Read(
            beam.io.BigQuerySource(query=f"""
                select cod_avion, capacidad, cod_tripulacion, cod_piloto, cod_vuelo, horario_salida, horario_llegada
                from (
                    SELECT 
                        cod_avion, capacidad, cod_tripulacion, cod_piloto, cod_vuelo, horario_salida, horario_llegada,
                        row_number() over (partition by cod_vuelo order by cod_tripulacion asc) as rn
                    FROM `{known_args.pj}.lake.vuelo`
                )
                where rn = 1;
            """,
                                   # This next stage of the pipeline maps the acct_number to a single row of
                                   # results from BigQuery.  Mapping this way helps Dataflow move your data around
                                   # to different workers.  When later stages of the pipeline run, all results from
                                   # a given account number will run on one worker.
                                   use_standard_sql=True))
        | 'Vuelo Details' >> beam.Map(
            lambda row: (
                row['cod_avion'], row
            )))

    ventas_query = data_lake_to_data_mart.get_ventas_query(known_args.pj)
    (p
     # Read the orders from BigQuery.  This is the source of the pipeline.  All further
     # processing starts with rows read from the query results here.
     | 'Read Ventas from BigQuery ' >> beam.io.Read(
        beam.io.BigQuerySource(query=ventas_query, use_standard_sql=True))
     # Here we pass in a side input, which is data that comes from outside our
     # main source.  The side input contains a map of states to their full name
     | 'Join with Pasajero' >> beam.Map(data_lake_to_data_mart.add_pasajero, AsDict(
        pasajero_source))
     | 'Join with Vuelo' >> beam.Map(data_lake_to_data_mart.add_vuelo, AsDict(
        vuelo_source)) 
     # | 'Set compensation' >> beam.Map(lambda s: set_compensation_value(s))
     | 'Abs amount' >> beam.Map(lambda s: get_absolute_value_amount(s))
     | 'Obfuscate last name' >> beam.Map(lambda s: obfuscate_data(s))
     # This is the final stage of the pipeline, where we define the destination
     # of the data.  In this case we are writing to BigQuery.
     | 'Write Data to BigQuery' >> beam.io.Write(
        beam.io.BigQuerySink(
            # The table name is a required argument for the BigQuery sink.
            # In this case we use the value passed in from the command line.
            known_args.output,
            # Here we use the JSON schema read in from a JSON file.
            # Specifying the schema allows the API to create the table correctly if it does not yet exist.
            schema=schema,
            # Creates the table in BigQuery if it does not yet exist.
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # Deletes all data in the BigQuery table before writing.
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
