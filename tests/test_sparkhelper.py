import sys
import unittest
from time import sleep

sys.path.append('../deploy')

from config import Config
from spark_helper import SparkHelper


def setup():
    config = Config("../config.yaml")
    config.config["spark_path"] = "./mock/spark"
    return SparkHelper(config)


def test_run_command():
    spark = setup()
    assert spark._run_command('echo "Hello world"') is None
    out, err = spark._run_command(["ping", "fake-url"])
    assert out == "" and err == "ping: cannot resolve fake-url: Unknown host\n"


def test_send_command():
    spark = setup()
    spark.ssh = [""]
    assert spark._run_command('echo "Hello world"') is None
    out, err = spark._run_command(["ping", "fake-url"])
    assert out == "" and err == "ping: cannot resolve fake-url: Unknown host\n"


def test_failed_launch_spark():
    spark = setup()
    spark.KEY_PAIR = "KEY_PAIR"
    spark.KEY_IDENT_FILE = "KEY_IDENT_FILE"
    spark.workers = "10"
    spark.instance = "r3.2xlarge"
    spark.spot_price = "0.20"
    spark.resume = True
    spark.name = "test-cluster"

    assert spark._setup_status is None
    output = SimpleOutput()
    _stdout = sys.stdout
    sys.stdout = output

    spark._launch_spark()

    sys.stdout = _stdout
    assert (output.get() ==
            "0\n1\n2\n['just-a-placeholder', '--key-pair=KEY_PAIR', "
            "'--identity-file=KEY_IDENT_FILE', '--region=us-east-1', "
            "'--zone=us-east-1b', '--slaves=10', '--instance-type=r3.2xlarge',"
            " '--hadoop-major-version=yarn', '--use-existing-master', "
            "'--spot-price=0.20', 'launch', '--resume', 'test-cluster']\n"
            "Failed!\n")
    assert spark._setup_status == spark.FAILED


def test_failed_setup_cluster():
    # TODO: how can we test sucessed launch?
    spark = setup()
    spark.KEY_PAIR = "KEY_PAIR"
    spark.KEY_IDENT_FILE = "KEY_IDENT_FILE"

    assert spark.get_setup_status() is None
    spark.setup_cluster("test-cluster", "10", "r3.xlarge",
                        "0.2", "passwd123", False)
    assert spark.get_setup_status() == spark.IN_PROCESS

    sleep(5)

    assert spark.get_setup_status() == spark.FAILED
    assert (spark.get_setup_log() ==
            "0\n1\n2\n['just-a-placeholder', '--key-pair=KEY_PAIR', "
            "'--identity-file=KEY_IDENT_FILE', '--region=us-east-1', "
            "'--zone=us-east-1b', '--slaves=10', '--instance-type=r3.xlarge',"
            " '--hadoop-major-version=yarn', '--use-existing-master', "
            "'--spot-price=0.20', 'launch', 'test-cluster']\n"
            "Failed!\n")
    assert not spark._thread_setup.is_running()
    assert type(spark.get_setup_duration()) is int

    spark.reset_spark_setup()
    assert spark._setup_status is None


def test_failed_setup_cluster_on_demand():
    spark = setup()
    spark.KEY_PAIR = "KEY_PAIR"
    spark.KEY_IDENT_FILE = "KEY_IDENT_FILE"

    assert spark.get_setup_status() is None
    spark.setup_cluster("test-cluster", "10", "r3.xlarge",
                        None, "passwd123", False)
    assert spark.get_setup_status() == spark.IN_PROCESS

    sleep(5)

    assert spark.get_setup_status() == spark.FAILED
    assert (spark.get_setup_log() ==
            "0\n1\n2\n['just-a-placeholder', '--key-pair=KEY_PAIR', "
            "'--identity-file=KEY_IDENT_FILE', '--region=us-east-1', "
            "'--zone=us-east-1b', '--slaves=10', '--instance-type=r3.xlarge',"
            " '--hadoop-major-version=yarn', '--use-existing-master', "
            "'launch', 'test-cluster']\nFailed!\n")
    assert not spark._thread_setup.is_running()
    assert type(spark.get_setup_duration()) is int
    print spark.get_setup_duration()

    spark.reset_spark_setup()
    assert spark._setup_status is None


class SimpleOutput():
    def __init__(self):
        self.output = []

    def write(self, s):
        self.output.append(s)

    def get(self):
        return ''.join(self.output)
