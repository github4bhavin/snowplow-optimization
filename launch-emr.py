import boto3
import base64


import json
import os
import uuid
import argparse

from datetime import datetime

BASE_DIR = '<base dir of this script>'

def get_json(json_file):
    with open(json_file, 'r') as f:
        return json.load(f)['data']

def get_base64_data(file):
    with open(file , 'rb') as f:
        all_lines = f.read().decode('utf-8')
        return base64.encodestring(all_lines.encode()).decode('utf-8').replace('\n','')

def get_steps(batch_etl_tstamp, run_etl_tstamp, etl_timestamp):

    return [
        {
            "Name":"(1/20) Setup Hadoop Debugging",
            "ActionOnFailure":"TERMINATE_CLUSTER",
            "HadoopJarStep":{
                "Jar":"s3://eu-west-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                "Args":[
                    "s3://eu-west-1.elasticmapreduce/libs/state-pusher/0.1/fetch"
                ]
            }
        },

        {
            "Name":"(2/20) archiving recovered data",
            "ActionOnFailure":"CONTINUE",
            "HadoopJarStep":{
                "Jar":"/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
                "Args":[
                    "--src","s3://<company-bucket>/processed/recovered/run={}".format(batch_etl_tstamp),
                    "--dest","s3://<company-bucket>/processed/archive/run={}".format(batch_etl_tstamp),
                    "--s3Endpoint","s3-eu-west-1.amazonaws.com"
                ]
            }
        },

        {
            "Name":"(3/20) s3://<company-bucket>/processed/recovered -> etl/processing ( staging )",
            "ActionOnFailure":"TERMINATE_CLUSTER",
            "HadoopJarStep" : {
                "Args":[
                    "--src", "s3://<company-bucket>/processed/recovered/run={}/".format(batch_etl_tstamp),
                    "--dest","s3://<company-bucket>/etl/processing/",
                    "--s3Endpoint","s3-eu-west-1.amazonaws.com",
                    "--srcPattern",".+",
                    "--deleteOnSuccess"
                ],
                "Jar":"/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
            }
        },

        {
            "Name":"(4/20) etl/processing (staging) -> Raw HDFS",
            "ActionOnFailure":"TERMINATE_CLUSTER",
            "HadoopJarStep" : {
                "Args":[
                    "--src","s3://<company-bucket>/etl/processing/",
                    "--dest","hdfs:///local/snowplow/raw-events/",
                    "--s3Endpoint","s3-eu-west-1.amazonaws.com"
                ],
                "Jar":"/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
            }
        },

        {
            "Name":"(5/20) Elasticity Spark Step: Enrich Raw Events",
            "ActionOnFailure":"TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Args":[
                    "spark-submit",
                    "--class","com.snowplowanalytics.snowplow.enrich.spark.EnrichJob",
                    "--master","yarn",
                    "--deploy-mode","cluster",
                    "s3://<company-hosted-assets>/3-enrich/spark-enrich/snowplow-spark-enrich-1.9.0.jar",
                    "--input-format","thrift",
                    "--etl-timestamp","{}".format(etl_timestamp),
                    "--iglu-config","{}".format(get_base64_data("../config/iglu-config.json")),
                    "--enrichments","{}".format(get_base64_data("../config/enrichments-config.json")),
                    "--input-folder","hdfs:///local/snowplow/raw-events/*",
                    "--output-folder","hdfs:///local/snowplow/enriched-events/",
                    "--bad-folder","s3://<company-bad-bucket>/spark/enriched/bad/run={}/".format(run_etl_tstamp)
                ],
                "Jar":"command-runner.jar"
            }
        },

        {
            "Name":"(6/20) Elasticity S3DistCp Step: Enriched HDFS -> S3",
            "ActionOnFailure":"TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Args":[
                    "--src","hdfs:///local/snowplow/enriched-events/",
                    "--dest","s3://<company-bad-bucket>/spark/enriched/good/run={}/".format(run_etl_tstamp),
                    "--srcPattern",".*part-.*",
                    "--s3Endpoint","s3-eu-west-1.amazonaws.com",
                    "--outputCodec","gzip"
                ],
                "Jar":"/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar"
            }
        },

        {
            "Name":"(7/20) Elasticity S3DistCp Step: Enriched HDFS _SUCCESS -> S3",
            "ActionOnFailure":"TERMINATE_CLUSTER",
            "HadoopJarStep" : {
                "Args":[
                    "--src","hdfs:///local/snowplow/enriched-events/",
                    "--dest","s3://<company-bad-bucket>/spark/enriched/good/run={}".format(run_etl_tstamp),
                    "--srcPattern",".*_SUCCESS",
                    "--s3Endpoint","s3-eu-west-1.amazonaws.com"
                ],
                "Jar":"/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar"
            }
        },

        {
            "Name":"(8/20) Elasticity Custom Jar Step: Empty Raw HDFS",
            "ActionOnFailure":"TERMINATE_CLUSTER",
            "HadoopJarStep" : {
                "Args":[
                    "s3://snowplow-hosted-assets/common/emr/snowplow-hadoop-fs-rmr-0.1.0.sh",
                    "hdfs:///local/snowplow/raw-events/"
                ],
                "Jar":"s3://eu-west-1.elasticmapreduce/libs/script-runner/script-runner.jar"
            }
        },

        {
            "Name":"(9/20) Elasticity Spark Step: Shred Enriched Events",
            "ActionOnFailure":"TERMINATE_CLUSTER",
            "HadoopJarStep" : {
            "Args":[
                "spark-submit",
                "--class","com.snowplowanalytics.snowplow.storage.spark.ShredJob",
                "--master","yarn",
                "--deploy-mode","cluster",
                "s3://snowplow-hosted-assets/4-storage/rdb-shredder/snowplow-rdb-shredder-0.12.0.jar",
                "--iglu-config","{}".format(get_base64_data("../config/iglu-config.json")),
                "--input-folder","hdfs:///local/snowplow/enriched-events/*",
                "--output-folder","hdfs:///local/snowplow/shredded-events/",
                "--bad-folder","s3://<company-bad-bucket>/spark/shredded/bad/run={}/".format(run_etl_tstamp)
            ],
            "Jar":"command-runner.jar"
            }
        },

        {
            "Name":"(10/20) Elasticity S3DistCp Step: Shredded HDFS -> S3",
            "ActionOnFailure":"TERMINATE_CLUSTER",
            "HadoopJarStep" : {
                "Args":[
                    "--src","hdfs:///local/snowplow/shredded-events/",
                    "--dest","s3://<company-bad-bucket>/spark/shredded/good/run={}/".format(run_etl_tstamp),
                    "--srcPattern",".*part-.*",
                    "--s3Endpoint","s3-eu-west-1.amazonaws.com",
                    "--outputCodec","gzip",
                    "--deleteOnSuccess"
                ],
                "Jar":"/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar"
            }
        },

        {
            "Name":"(11/20) Elasticity S3DistCp Step: Shredded HDFS _SUCCESS -> S3",
            "ActionOnFailure":"TERMINATE_CLUSTER",
            "HadoopJarStep":{
            "Args":[
                "--src","hdfs:///local/snowplow/shredded-events/",
                "--dest","s3://<company-bad-bucket>/spark/shredded/good/run={}/".format(run_etl_tstamp),
                "--srcPattern",".*_SUCCESS",
                "--s3Endpoint","s3-eu-west-1.amazonaws.com",
                "--deleteOnSuccess"
            ],
            "Jar":"/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar"
            }
        },

        {
            "Name":"(12/20) Elasticity S3DistCp Step: Raw Staging S3 -> Raw Archive S3",
            "ActionOnFailure":"TERMINATE_CLUSTER",
            "HadoopJarStep" : {
                "Args":[
                    "--src","s3://<company-bucket>/etl/processing/",
                    "--dest","s3://<company-bad-bucket>/spark/archive/run={}/".format(run_etl_tstamp),
                    "--s3Endpoint","s3-eu-west-1.amazonaws.com",
                    "--deleteOnSuccess"
                ],
                "Jar":"/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar"
            }
        },

        {
          "Name":"(13/20) Elasticity Custom Jar Step: Load iroko_redshift Storage Target",
          "ActionOnFailure":"TERMINATE_CLUSTER",
          "HadoopJarStep": {
              "Args":[
                "--config","{}".format(get_base64_data("../config/run-configs/{}.yml".format(batch_etl_tstamp))),
                "--resolver","{}".format(get_base64_data("../config/iglu-config.json")),
                "--logkey","s3://<company-bad-bucket>/spark_etl_logs/rdb-loader/{}/{}".format(run_etl_tstamp, uuid.uuid1() ),
                "--target","{}".format(get_base64_data("../config/targets/atomic.json"))
              ],
              "Jar":"s3://snowplow-hosted-assets/4-storage/rdb-loader/snowplow-rdb-loader-0.12.0.jar"
           }
        },

        {
            "Name":"(14/20) Elasticity S3DistCp Step: Enriched S3 -> Enriched Archive S3",
            "ActionOnFailure":"TERMINATE_CLUSTER",
            "HadoopJarStep":{
                "Args":[
                    "--src","s3://<company-bad-bucket>/spark/enriched/good/run={}/".format(run_etl_tstamp),
                    "--dest","s3://<company-bad-bucket>/spark/enriched/archive/run={}/".format(run_etl_tstamp),
                    "--s3Endpoint","s3-eu-west-1.amazonaws.com",
                    "--deleteOnSuccess"
                ],
                "Jar":"/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar"
            }
        },

        {
            "Name":"(15/20) Elasticity S3DistCp Step: Shredded S3 -> Shredded Archive S3",
            "ActionOnFailure":"TERMINATE_CLUSTER",
            "HadoopJarStep":{
                "Args":[
                    "--src","s3://<company-bad-bucket>/spark/shredded/good/run={}/".format(run_etl_tstamp),
                    "--dest","s3://<company-bad-bucket>/spark/shredded/archive/run={}/".format(run_etl_tstamp),
                    "--s3Endpoint","s3-eu-west-1.amazonaws.com",
                    "--deleteOnSuccess"
                ],
                "Jar":"/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar"
            }
        }
    ]

def launch_cluster_with_steps(batch_etl_tstamp):

    ep = datetime.utcfromtimestamp(0)
    dt = datetime.now()
    run_etl_tstamp = dt.strftime("%Y-%m-%d-%H-%M-%S")
    etl_timestamp = int( (dt - ep).total_seconds() * 1000 )

    emr = boto3.client('emr')
    print(\
    emr.run_job_flow(
        Name="Snowplow ETL (recovery) [{b}]->[{e}]".format(b=batch_etl_tstamp, e=run_etl_tstamp),
        LogUri="s3n://<company-bad-bucket>/spark_etl_logs/",
        #AmiVersion="5.5.0",
        ReleaseLabel="emr-5.5.0",
        Instances={
                        "InstanceGroups"               : [
                            {
                                "Name": "Master",
                                "Market": "ON_DEMAND",
                                "InstanceRole": "MASTER",
                                "InstanceType": "m1.medium",
                                "InstanceCount": 1,
                                "Configurations": [],
                                "EbsConfiguration": {
                                    "EbsBlockDeviceConfigs" : [
                                        {
                                            "VolumesPerInstance": 12,
                                            "VolumeSpecification": {
                                                "Iops" : 100,
                                                "SizeInGB" : 10,
                                                "VolumeType": "io1"
                                            }
                                        }
                                    ]
                                }
                            },

                            {
                                "Name": "CORE",
                                "Market": "ON_DEMAND",
                                "InstanceRole": "CORE",
                                "InstanceType": "r3.4xlarge",
                                "InstanceCount": 3,
                                "Configurations": [],
                                "EbsConfiguration": {
                                    "EbsBlockDeviceConfigs" : [
                                        {
                                            "VolumesPerInstance": 12,
                                            "VolumeSpecification": {
                                                "Iops" : 100,
                                                "SizeInGB" : 10,
                                                "VolumeType": "io1"
                                            }
                                        }
                                    ]
                                }
                            }
                        ],
                        "Ec2KeyName"                   : '<key-name>',
                        "Placement"                    : {
                            "AvailabilityZone": "eu-west-1c"
                        },
                        "KeepJobFlowAliveWhenNoSteps"  : False,
                        "EmrManagedMasterSecurityGroup": "<sg-name>",
                        "EmrManagedSlaveSecurityGroup" : "<sg-name>",
        },
            BootstrapActions=[
                {
                    "Name" : "Install Thriftpy",
                    "ScriptBootstrapAction": {
                        "Path": "s3://<company-bucket>/scripts/bootstrap-emr.sh",
                        "Args": []
                    }
                },

                {
                    "Name": "Tracker Update Bootstrap Action",
                    "ScriptBootstrapAction": {
                        "Path": "s3://<company-bucket>/scripts/bootstrap-emr.sh"
                    }
                }
        ],
        Applications=[
            {"Name": "Hadoop"},
            {"Name": "Spark"},
            {"Name": "Ganglia"}
        ],
        Tags=[
            {"Key": "owner", "Value": "owner-tag"},
            {"Key": "product", "Value": "snowplow-recovery"}
        ],
        ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        Configurations=[
            {
                "Classification": "core-site",
                "Properties" : {
                    "Io.file.buffer.size": "65536"
                }
            },
            {
                "Classification": "mapred-site",
                "Properties": {
                    "Mapreduce.user.classpath.first": "true"
                }
            },
            {
                "Classification": "yarn-site",
                "Properties": {
                    "yarn.resourcemanager.am.max-attempts": "1"
                }
            },
            {
                "Classification": "spark",
                "Properties":{
                    "maximizeResourceAllocation": "true"
                }
            },
            {
                "Classification": "spark-defaults",
                "Properties": {

                    "spark.driver.memory": "27G",
                    "spark.driver.cores": "3",

                    "spark.executor.memory": "27G",
                    "spark.executor.cores": "3",
                    "spark.executor.instances":"11",

                    "spark.default.parallelism": "33",

                    "spark.yarn.executor.memoryOverhead": "3072",
                    "spark.yarn.driver.memoryOverhead": "3072",

                    "spark.dynamicAllocation.enabled": "false"
                }
            }
        ],
        Steps=get_steps(batch_etl_tstamp, run_etl_tstamp, etl_timestamp)
    ))

if __name__ == '__main__':

    parser = argparse.ArgumentParser('run-etl-py')
    parser.add_argument('--etl', action='store', help="etl process")

    args = parser.parse_args()

    if args.etl is None:
        print('--etl is required')
        exit(1)


    batch_etl_tstamp = args.etl

    launch_cluster_with_steps(batch_etl_tstamp)
