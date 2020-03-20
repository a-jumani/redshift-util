import argparse
import boto3
import configparser
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

# create relevant clients
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
