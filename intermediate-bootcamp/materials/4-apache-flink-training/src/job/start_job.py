from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.udf import ScalarFunction, udf
import os
import json
import requests
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
def create_processed_events_sink_kafka(t_env):
    """similar to source but different topic name"""
    table_name = "process_events_kafka"
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    sasl_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_GROUP').split('.')[0] + '.' + table_name}',
            'properties.ssl.endpoint.identification.algorithm' = '',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.jaas.config' = '{sasl_config}',
            'format' = 'json'
        );
        """
    print(sink_ddl)
    t_env.execute_sql(sink_ddl)
    return table_name


def create_processed_events_sink_postgres(t_env):
    """tell Flink that the table has this schema but an actual table still needs to be created"""
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp TIMESTAMP(3),
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    # driver is required to connect to postgres
    t_env.execute_sql(sink_ddl)
    return table_name

class GetLocation(ScalarFunction): # ScalarFunction-Flink parent class: get 1 row, return 1 row
  """return ip location from the website with API in env"""
  def eval(self, ip_address):
    url = "https://api.ip2location.io"
    response = requests.get(url, params={
        'ip': ip_address,
        'key': os.environ.get("IP_CODING_KEY")
    })

    if response.status_code != 200:
        # Return empty dict if request failed
        return json.dumps({})

    data = json.loads(response.text)

    # Extract the country and state from the response
    # This might change depending on the actual response structure
    country = data.get('country_code', '')
    state = data.get('region_name', '')
    city = data.get('city_name', '')
    return json.dumps({'country': country, 'state': state, 'city': city})

get_location = udf(GetLocation(), result_type=DataTypes.STRING())



def create_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "events"
    pattern = "yyyy-MM-dd HH:mm:ss"
    # event_timestamp need pattern to convert from string to datetime
    source_ddl = f"""
        CREATE TABLE {table_name} (
            url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            host VARCHAR,
            ip VARCHAR,
            headers VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    # connector: 'kafka', 'rabbitmq', 'jdbc' (postgres or sql db)
    # properties.bootstrap.servers = a cluster of server of Kafka
    # topic = a table name
    # properties.ssl.endpoint.identification.algorithm - algorithm to identify if user is authorized, work with truststore,keystore: 'https'
    # properties.ssl.truststore.location -path to the jks file
    # properties.ssl.truststore.password -Kafka password
    # properties.ssl.keystore.location -path to the jks file
    # properties.ssl.keystore.password -Kafka password'
    # properties.group.id = to group kafka's topics under a name
    # properties.security.protocol for security of Kafka: 'SASL_SSL', 'SSL'
    # scan.startup.mode- the offset in lesson 1 = 'latest-offset', 'earliest-offset'
    # properties.auto.offset.reset is required but if there is checkpoint, will follow checkpoint first
    # format -how data is stored: 'json', 'csv', 'tsv'

    print(source_ddl)
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    """Main function"""
    print('Starting Job!')
    # Set up the execution environment
    # get_execution_environment().enable_batch_mode() for batch
    env = StreamExecutionEnvironment.get_execution_environment()
    print('got streaming environment')
    env.enable_checkpointing(10 * 1000) # 10 millsec -> 10 sec
    env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build() # tell it to use streaming
    t_env = StreamTableEnvironment.create(env, environment_settings=settings) # define the source environment to use in the entire session
    t_env.create_temporary_function("get_location", get_location) # similar to spark dataframe to use udf
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        kafka_sink = create_processed_events_sink_kafka(t_env)
        postgres_sink = create_processed_events_sink_postgres(t_env)

        print('loading into kafka ..')
        t_env.execute_sql(
            f"""
                    INSERT INTO {kafka_sink}
                    SELECT
                        JSON_VALUE(headers, '$.x-forwarded-for') as ip,
                        DATE_FORMAT(event_timestamp, 'yyyy-MM-dd HH:mm:ss') as event_timestamp,
                        referrer,
                        host,
                        url,
                        get_location(JSON_VALUE(headers, '$.x-forwareded-for')) as geodata
                    FROM {source_table}
                    """
        )

        print('loading into postgres ..')
        t_env.execute_sql(
            f"""
                    INSERT INTO {postgres_sink}
                    SELECT
                        JSON_VALUE(headers, '$.x-forwarded-for') as ip,
                        event_timestamp,
                        referrer,
                        host,
                        url,
                        get_location(JSON_VALUE(headers, '$.x-forwareded-for')) as geodata
                    FROM {source_table}
                    """
        ).wait()
    # wait() to tell to not stop after job ending otherwise it will run all current data in Kafka queue and close the job

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
