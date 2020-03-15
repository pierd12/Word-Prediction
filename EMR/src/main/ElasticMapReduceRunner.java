package main;


import java.io.FileInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.waiters.WaiterParameters;

import org.apache.log4j.PropertyConfigurator;

public class ElasticMapReduceRunner {

    public static DefaultAWSCredentialsProviderChain credentialsProvider;
    public static String bucket_name ="pierd";
    public static AWSCredentials credentials_profile = null;
    public static AmazonS3 s3Client ; 
    public static void main(String[] args) throws FileNotFoundException, IOException {
        System.out.println("ElasticMapReduceRunner :: has just started..");
        System.out.println("ElasticMapReduceRunner :: reading AWSCredentials properties file...");

        try {
            credentials_profile = new ProfileCredentialsProvider("default").getCredentials(); // specifies any named profile in .aws/credentials as the credentials provider
        }
        catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load credentials from .aws/credentials file. " +
                            "Make sure that the credentials file exists and that the profile name is defined within it.",
                    e);
        }
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials_profile))
                .withRegion(Regions.US_WEST_2)
                .build();
        // create an EMR client using the credentials and region specified in order to create the cluster
        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials_profile))
                .withRegion(Regions.US_EAST_1)
                .build();
        List<StepConfig> steps = makeconfigs();
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.C5d2xlarge.toString())
                .withSlaveInstanceType(InstanceType.C5dXlarge.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("dam")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                //.withServiceRole("EMR_DefaultRole")
                //.withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.28.0")
                .withName("CalProb")
                .withInstances(instances)
                .withSteps(steps)
                .withLogUri("s3://pierd/logs/")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        
        System.out.println("ElasticMapReduceRunner :: successfully ran a job on Amazon Elastic Map Reduce");
        System.out.println("ElasticMapReduceRunner :: Ran job flow with id: " + jobFlowId);
     
        DescribeClusterRequest describeRequest = new DescribeClusterRequest()
            .withClusterId(runJobFlowResult.getJobFlowId());

        // Wait until terminated
        emr.waiters().clusterTerminated().run(new WaiterParameters(describeRequest));
        
    }
    public static List<StepConfig> makeconfigs(){
        List<StepConfig> steps = new LinkedList<StepConfig>();
        String stepargs1[] = new String[2];
        //s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data
        stepargs1[0]= "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/"; stepargs1[1]= "output1/";
        steps.add(makestep(stepargs1,"wordcount.jar","WordCount"));
        String stepargs2[] = new String[2];
        stepargs2[0]= "s3://pierd/output1/"; stepargs2[1]= "output2/";
        steps.add(makestep(stepargs2,"join.jar","Join"));
        String stepargs3[] = new String[2];
        stepargs3[0]= "s3://pierd/output2/"; stepargs3[1]= "output/";
        steps.add(makestep(stepargs3,"sorter.jar","Sorter"));
        return steps;
    }
    public static StepConfig makestep(String[] args ,String jarname,String mainclass)
    {
    	HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3://pierd/ass2-"+jarname)
                .withMainClass("main."+mainclass)
                .withArgs(args[0], "s3://pierd/"+args[1]);

        StepConfig stepConfig = new StepConfig()
                .withName(mainclass)
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        return stepConfig;
    }
    @SuppressWarnings("deprecation")
	public static void clean_bucket(String folder_name)
    {
    	
    	if (s3Client.doesBucketExistV2(bucket_name)) {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(bucket_name)
                    .withPrefix(folder_name);

            ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);

            while (true) {
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    s3Client.deleteObject(bucket_name, objectSummary.getKey());
                }
                if (objectListing.isTruncated()) {
                    objectListing = s3Client.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }
        }
    }

}
