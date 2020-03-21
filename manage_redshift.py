import argparse
import boto3
import configparser
import json
import textwrap
import time

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


def startCluster():
    """ Spin up a Redshift cluster.
    """
    # attempt to create a cluster
    print("Creating a Redshift cluster...")
    try:
        redshift.create_cluster(

            # hardware parameters
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # database access configuration
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # accesses
            IamRoles=[iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]]
        )
    except Exception as e:
        print(e)
        return

    # wait for cluster to spin up
    print("Waiting for cluster to be available...")
    while redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]["ClusterStatus"] != "available":
        time.sleep(30)
        print("\tChecking status again...")


def enableRemoteAccess():
    """ Enable remote access to the cluster.
    """
    pass


def terminateCluster():
    """ Delete the cluster and clean up resources
    """
    pass


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

    # argument to denote action to take
    action.add_argument(
        "--action",
        help=textwrap.dedent(
            """\
            execute step n, where n is from 1..4 and means:
                step 1: create IAM role
                step 2: spin up cluster
                step 3: enable remote access
                step 4: delete cluster
            """
            ),
        type=int,
        choices=(1, 2, 3, 4),
        metavar="n"
    )

    args = parser.parse_args()

    if args.status:
        status()
    elif args.action:
        [addS3AccessRole, startCluster,
         enableRemoteAccess, terminateCluster][args.action-1]()
