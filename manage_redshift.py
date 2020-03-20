import argparse
import boto3
import configparser
import json
import textwrap


config = configparser.ConfigParser()
config.read_file(open("config.cfg"))

# obtain AWS credentials
KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")

# obtain cluster deployment config
DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

# obtain cluster access config
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")

# IAM role name for Redshift cluster
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

# create relevant clients
iam = boto3.client(
    "iam",
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)

redshift = boto3.client(
    "redshift",
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)


def status():
    """ Get cluster status.
    """
    # get cluster properties
    try:
        myClusterProps = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    except Exception as e:
        print(e)
        return

    # print some properties
    propsToPrint = ["ClusterIdentifier", "NodeType", "NumberOfNodes",
                    "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                    "VpcId"]
    for k in propsToPrint:
        try:
            print(f"{k:20} {myClusterProps[k]}")
        except KeyError:
            pass


def addS3AccessRole():
    """ Enable read-only access to S3 bucket for a Redshift cluster
    using IAM role.
    """
    # create a new role
    try:
        print("Creating a new IAM Role...")
        iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to access AWS services.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"}
                        }
                    ],
                    "Version": "2012-10-17"
                }
            )
        )
    except Exception as e:
        print(e)
        return

    # attach S3 read-only access policy to the new role
    print("Attaching S3 read-only access policy...")
    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )["ResponseMetadata"]["HTTPStatusCode"]


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent("\
            Manage your Redshift cluster. Use config.cfg for configuration.")
    )
    action = parser.add_mutually_exclusive_group(required=True)

    # argument for cluster status
    action.add_argument(
        "--status",
        action="store_true",
        help="get cluster status"
    )

    args = parser.parse_args()

    if args.status:
        status()
