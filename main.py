import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions # used to specify options for pipeline run
from apache_beam.io.gcp.internal.clients import bigquery # bigquery  name and schema definition
import logging
import argparse



# pipeline options defines the rules for a pipeline to run

pipeline_options =PipelineOptions(temp_location='gs://jaya_final_capstone_test_bucket/',
                                  project="york-cdf-start",job_name="jaya-mohan-final-job",
                                  runner="DataflowRunner",
                                  region="us-central1")

# Main Method runs the script for table creation
def run(argv=None):
    try:
        #passing command line argument
        #arguments included
        # 1. project id
        # 2. dataset id
        # 3. table name with products view
        # 4. table name with orders view
        # 5. query for customer and product input
        # 6. query for customer and order input
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--projectID',
            required=False,
            default='york-cdf-start',
            help=(
                'Input BigQuery table to process specified as: '
                'PROJECT'))
        parser.add_argument(
            '--datasetID',
            required=False,
            default='final_jaya_mohan',
            help=(
                'Input BigQuery table to process specified as: '
                'DATASET'))

        parser.add_argument(
            '--TableProduct',
            required=False,
            default='cust_tier_code-sku-total_no_of_product',
            help=('Input BigQuery table to process specified as: '
                'TABLE for product'))
        parser.add_argument(
            '--TableSales',
            required=False,
            default='cust_tier_code-sku-total_sales_amount',
            help=('Input BigQuery table to process specified as: '
                'TABLE for sales'))
        parser.add_argument(
            '--queryProduct',
            required=False,
            default="""select cast(c.CUST_TIER_CODE as STRING) as CUST_TIER_CODE ,p.SKU,count(c.CUSTOMER_ID) as total_no_of_product_views FROM `york-cdf-start.final_input_data.product_views` as p             
                join `york-cdf-start.final_input_data.customers` as c 
                on p.CUSTOMER_ID=c.CUSTOMER_ID 
                group by c.CUST_TIER_CODE,p.SKU""",
            help=('Input BigQuery table to process specified as: '
                  'TABLE for product'))
        parser.add_argument(
            '--queryOrder',
            required=False,
            default=""" SELECT cast(c.CUST_TIER_CODE as STRING) as CUST_TIER_CODE,o.SKU,sum(o.ORDER_AMT) as total_sales_amount  FROM `york-cdf-start.final_input_data.orders` as o 
                join `york-cdf-start.final_input_data.customers` as c 
                on o.CUSTOMER_ID=c.CUSTOMER_ID group by o.SKU,c.CUST_TIER_CODE
                """,
            help=('Input BigQuery table to process specified as: '
                  'TABLE for sales'))

        known_args, pipeline_args = parser.parse_known_args(argv)
        # Table's name Specification
        table_total_Product_views_spec = bigquery.TableReference(
            projectId=known_args.projectID,
            datasetId=known_args.datasetID,
            tableId=known_args.TableProduct)

        table_total_sales_spec = bigquery.TableReference(
            projectId = known_args.projectID,
            datasetId = known_args.datasetID,
            tableId = known_args.TableSales)

        # Table's schema Specification
        Product_Views_schema = {
            'fields': [
                {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
                 {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                {'name': 'total_no_of_product_views', 'type': 'INTEGER', 'mode': 'REQUIRED'}
            ]}
        total_sales_schema = {
            'fields': [
                {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                {'name': 'total_sales_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'}
            ]}
    #pipeline Intialization
        with beam.Pipeline(options=pipeline_options) as pipeline:
            #Read from table customer and product
            #sql script does selection with aggregation and cast
                product_result = pipeline | "read from table cust,product views" >> beam.io.ReadFromBigQuery(query= known_args.queryProduct
    ,project=known_args.projectID,use_standard_sql=True)


            # Read from table customer and order
            #  sql script does selection with aggregation and cast
                total_sales_result=pipeline|"read from table cust,order" >> beam.io.ReadFromBigQuery(query=known_args.queryOrder,project=known_args.projectID,use_standard_sql=True)

             #Write to Bigquery & creating new tables
                product_result|"write to table total_no_of_product_views ">> beam.io.WriteToBigQuery(table=table_total_Product_views_spec,schema=Product_Views_schema,write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

                total_sales_result|"write to table total_sales_amount">> beam.io.WriteToBigQuery(table=table_total_sales_spec,schema=total_sales_schema,write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    except:

        print("Error in run")

# test implementation:
class convertion_Pardo():
    def process (self,element):
        if 'cust_tier_code' in element:
            element['cust_tier_code'] = str(element['cust_tier_code'])
        return [element]

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
