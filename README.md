# ETL con Dataflow en python

## Descripción general

En este pequeño proyecto se creó 3 pipelines de datos con python que recopilan datos de un conjunto de datos
disponibilizados en GCS y BigQuery mediante estos servicios de Google Cloud:

- GCS
- Dataflow
- BigQuery

## Activar CloudShell

En Cloud Console, en la barra de herramientas superior derecha, haga clic en el botón Activar
Cloud Shell. Y luego Continuar

Se tarda unos minutos en aprovisionar y conectarse al entorno. Cuando está conectado, ya
está autenticado y el proyecto se establece en su PROJECT_ID.

Usted puede listar los id de proyecto con el siguiente comando:

``` bash
gcloud config list project
```

## Descarga el código

#### **Clonar el repositorio**

``` bash
git clone https://github.com/jeremyruizacevedo/dataflow-python-etl.git
```

Ahora configure una variable igual a su ID de proyecto, reemplazando <YOUR-PROJECT-ID> con
su ID de proyecto

``` bash
export PROJECT=<YOUR-PROJECT-ID>
```
``` bash
gcloud config set project $PROJECT
```

## Crear depósito de almacenamiento en la nube

Usa el comando make bucket para crear un nuevo depósito regional en la región us-central1
dentro de tu proyecto:

``` bash
gsutil mb -c regional -l us-central1 gs://$PROJECT
```

## Copie archivos a su Bucket

Usa el comando gsutil para copiar archivos en el bucket de Cloud Storage que acabas de crear:

``` bash
cd dataflow-python-etl/
```
``` bash
gsutil cp -r data_files/ gs://$PROJECT/
```

## Crear el dataset en BigQuery
Crea un conjunto de datos en BigQuery llamado lake. Aquí es donde se cargarán todas tus
tablas en BigQuery:

``` bash
bq mk lake
```

# Ejecutar el pipeline de Apache Beam

Correr los siguientes comandos para configurar el entorno de Python:

``` bash
sudo pip install virtualenv
```
``` bash
virtualenv -p python3 venv
```
``` bash
source venv/bin/activate
```
``` bash
pip install apache-beam[gcp]
```
``` bash
pip install --upgrade google-cloud-bigquery
```

## Ingesta de Datos

Se ejecutará un pipeline de Dataflow con un origen de TextIO y un destino de
BigQueryIO para ingerir datos en BigQuery. Más específicamente, se hará lo
siguiente:

- Leer los archivos de Cloud Storage.
- Filtrar la fila de encabezado en los archivos.
- Convertir las líneas leídas en objetos de diccionario.
- Enviar las filas a BigQuery.

Ejecute el siguiente comando: (el flag --experiment use_unsupported_python_version es
siempre y cuando tenga versions de python >= 3.9)

``` bash
python src/data_ingest.py --project=$PROJECT --region=us-central1 --runner=DataflowRunner --input gs://$PROJECT/data_files/pasajero.csv,gs://$PROJECT/data_files/vuelo.csv,gs://$PROJECT/data_files/venta.csv --save_main_session --experiment use_unsupported_python_version
```

## Transformación de datos

Ahora se ejecutará un pipeline de Dataflow que leerá datos de 3 fuentes de datos de BigQuery
y luego las unirá. Específicamente:
- Leerá 3 tablas de BigQuery.
- Unirá las 3 fuentes de datos.
- Filtrará la fila de encabezado en los archivos.
- Convierta las líneas leídas en objetos de diccionario.
- Aplicará ciertas transformaciones
- Envía las filas a BigQuery.

Ejecute los siguientes comandos:
Para todas las condiciones en el ejercicio 2 excepto lo de compensacion

``` bash
python src/datalake_to_mart.py --worker_disk_type="compute.googleapis.com/projects//zones//diskTypes/pd-ssd" --max_num_workers=4 --project=$PROJECT --runner=DataflowRunner --save_main_session --region=us-central1 --experiment use_unsupported_python_version
```

Para concluir, corremos el siguiente comando para dar valor a la columna compensación

``` bash
python src/datalake_compensation.py --worker_disk_type="compute.googleapis.com/projects//zones//diskTypes/pd-ssd" --max_num_workers=4 --project=$PROJECT --runner=DataflowRunner --save_main_session --region=us-central1 --experiment use_unsupported_python_version
```
