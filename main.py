import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery



pipeline_options =PipelineOptions(temp_location='gs://jaya_final_capstone_test_bucket/',project="york-cdf-start",job_name="jaya-mohan-final-job",runner="DataflowRunner",region="us-central1")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #try:
    table_total_Product_views_spec = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='final_jaya_mohan',
        tableId='cust_tier_code-sku-total_no_of_product')

    table_total_sales_spec = bigquery.TableReference(
        projectId = 'york-cdf-start',
        datasetId = 'final_jaya_mohan',
        tableId = 'cust_tier_code-sku-total_sales_amount')


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

    with beam.Pipeline(options=pipeline_options) as pipeline:
            product_result = pipeline | "read from table cust,product views" >> beam.io.ReadFromBigQuery(query= """select cast(c.CUST_TIER_CODE as STRING) as CUST_TIER_CODE ,p.SKU,count(c.CUSTOMER_ID) as total_no_of_product_views FROM `york-cdf-start.final_input_data.product_views` as p             
            join `york-cdf-start.final_input_data.customers` as c 
            on p.CUSTOMER_ID=c.CUSTOMER_ID 
            group by c.CUST_TIER_CODE,p.SKU"""
,project="york-cdf-start",use_standard_sql=True)

            total_sales_result=pipeline|"read from table cust,total sales" >> beam.io.ReadFromBigQuery(query=""" SELECT cast(c.CUST_TIER_CODE as STRING) as CUST_TIER_CODE,o.SKU,sum(o.ORDER_AMT) as total_sales_amount  FROM `york-cdf-start.final_input_data.orders` as o 
            join `york-cdf-start.final_input_data.customers` as c 
            on o.CUSTOMER_ID=c.CUSTOMER_ID group by o.SKU,c.CUST_TIER_CODE
            """,project="york-cdf-start",use_standard_sql=True)


            product_result|"write to table total_no_of_product_views ">> beam.io.WriteToBigQuery(table=table_total_Product_views_spec,schema=Product_Views_schema,write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

            total_sales_result|"write to table total_sales_amount">> beam.io.WriteToBigQuery(table=table_total_sales_spec,schema=total_sales_schema,write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)


    #except:

        #print("Error in main")

