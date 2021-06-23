import boto3
import configparser
import json

def create_bucket(s3_client, bucket_name):
    """Creates a bucket on AWS S3.
    Args:
        s3_client: S3 Client
        bucket_name: Name of the bucket
    Returns:
        None
    """
    location = {'LocationConstraint': 'us-west-2'}
    s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)


def upload_etl(s3_client, file_name, bucket_name):
    """Upload ETL file to run on AWS.
    Args:
        s3_client: S3 Client
        file_name: The ETL file
        bucket_name: Name of the bucket
    Returns:
        None
    """
    s3_client.upload_file(file_name, bucket_name, 'etl.py')


def upload_files(s3_client, bucket_name):
    """Upload all data files to S3.
    Args:
        s3_client: S3 Client
        bucket_name: Name of the bucket
    Returns:
        None
    """
    s3_client.upload_file('i94_apr16_sub.sas7bdat', bucket_name, 'i94_apr16_sub.sas7bdat')
    s3_client.upload_file('GlobalLandTemperaturesByCity.csv', bucket_name, 'temperatures_data/GlobalLandTemperaturesByCity.csv')
    s3_client.upload_file('us-cities-demographics.csv', bucket_name, 'demographics_data/us-cities-demographics.csv')
    s3_client.upload_file('airport-codes_csv.csv', bucket_name, 'airports_data/airport-codes_csv.csv')
   


def create_emr_cluster(emr_client, config):
    """Creates a EMR Cluster AWS S3.
    Args:
        emr_client: EMR Client
        config: Configuration file
    Returns:
        None
    """
    cluster_id = emr_client.run_job_flow(
        Name='hieu-spark-cluster',
        ReleaseLabel='emr-5.33.0',
        LogUri='s3://aws-logs-594695117986-us-west-2/elasticmapreduce/',
        Applications=[
            {
                'Name': 'Spark'
            },
            {
                'Name': 'Zeppelin'
            },
            {
                'Name': 'JupyterHub'
            },
            {
                'Name': 'JupyterEnterpriseGateway'
            },
        ],
        Configurations=[
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3"
                        }
                    }
                ]
            }
        ],
        Instances={
            'Ec2SubnetId': 'subnet-b6c56afc',
            'Ec2KeyName': 'spark-cluster',
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 5,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        Steps=[
            {
                'Name': 'Setup Debugging',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['state-pusher-script']
                }
            },
            {
                'Name': 'Setup - copy files',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', 's3://' + config['BUCKET']['CODE_BUCKET'], '/home/hadoop/',
                             '--recursive']
                }
            },
            {
                'Name': 'Run Spark',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '/home/hadoop/etl.py',
                             config['S3']['INPUT_DATA'], config['S3']['OUTPUT_DATA']]
                }
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='HieuLeEmrRole'
    )


def create_iam_role(iam_client):
    role = iam_client.create_role(
        RoleName='MyEmrRole',
        Description='Allows EMR to call AWS services on your behalf',
        AssumeRolePolicyDocument=json.dumps({
            'Version': '2012-10-17',
            'Statement': [{
                'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'elasticmapreduce.amazonaws.com'}
            }]
        })
    )

    iam_client.attach_role_policy(
        RoleName='HieuLeEmrRole',
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
    )

    iam_client.attach_role_policy(
        RoleName='HieuLeEmrRole',
        PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
    )

    return role

def main():
    """Main Script to setup the cluster and bucket
    Args:
        None
        
    Returns:
        None
    """
    config = configparser.ConfigParser()

    config.read('dl.cfg')

    # iam_client = boto3.client('iam')
    # create_iam_role(iam_client)
    
    s3_client = boto3.client(
        's3',
        region_name='us-west-2',
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'],
    )

    # create_bucket(s3_client, config['BUCKET']['OUTPUT_BUCKET'])
    
    # create_bucket(s3_client, config['BUCKET']['CODE_BUCKET'])

    # create_bucket(s3_client, config['BUCKET']['INPUT_BUCKET'])

    # upload_files(s3_client,config['BUCKET']['INPUT_BUCKET'])

    upload_etl(s3_client, 'etl.py', config['BUCKET']['CODE_BUCKET'])

    
    emr_client = boto3.client(
            'emr',
            region_name='us-west-2',
            aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY']
        )
    
    create_emr_cluster(emr_client, config)
    

if __name__ == '__main__':
    main()