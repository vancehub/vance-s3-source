package cloud.linkall.vance.s3;

import cloud.linkall.common.constant.ConfigConstant;
import cloud.linkall.common.file.GenericFileUtil;
import cloud.linkall.common.string.StringUtil;
import cloud.linkall.core.builder.aws.AwsHelper;
import cloud.linkall.core.http.DeliveryClient;
import io.cloudevents.CloudEvent;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import software.amazon.awssdk.services.s3.model.NotificationConfiguration;
import software.amazon.awssdk.services.s3.model.PutBucketNotificationConfigurationRequest;
import software.amazon.awssdk.services.s3.model.QueueConfiguration;
import software.amazon.awssdk.services.sqs.SqsClient;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sts.StsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3Source {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3Source.class);
    public static void main(String[] args) {

        JsonObject userConfig = null;
        JsonObject quePolicy = null;
        //read config from outside first, if it doesn't exist, then read local resource
        try {
            userConfig = new JsonObject(GenericFileUtil.readFile(ConfigConstant.DEFAULT_CONFIG_NAME));
        } catch (IOException e) {
            LOGGER.debug(e.getMessage());
        }
        if(null == userConfig){
            try {
                userConfig = new JsonObject(GenericFileUtil.readResource("config.json"));
                quePolicy = new JsonObject(GenericFileUtil.readResource("queue_policy.json"));
            } catch (IOException e) {
                LOGGER.debug(e.getMessage());
                e.printStackTrace();
            }
        }

        // get region
        Region region = AwsHelper.getRegion(userConfig.getString("region"));
        // get S3Client
        S3Client s3 = Objects.requireNonNull(
                S3Client.builder().region(region).build());
        SqsClient sqsClient = SqsClient.builder().region(region).build();
        //get stsClient
        StsClient stsClient = StsClient.builder()
                .region(region)
                .build();
        String accountId = AwsHelper.getAccountInfo(stsClient, "AKIA4IXKMXC7HGMO24GN");

        //get sqs ARN and S3 ARN
        String sqsArn = userConfig.getString("SQS_ARN");
        String s3Arn = userConfig.getString("S3_Bucket_ARN");
        String sink = userConfig.getString("sink");
        String queUrl = null;
        //if SQS_ARN is omitted,we create a queue for users
        if(null==sqsArn||"".equals(sqsArn)){
            queUrl = vanceQueueUrl(sqsClient);
            //vance-sqs doesn't exist
            if(queUrl ==null){
                //create a vance-sqs and return its queUrl
                String suffix = StringUtil.randomAlphabetic(5);
                String queueName = "vance-s3-sqs-"+ suffix;
                queUrl = AwsHelper.createQueue(sqsClient,queueName);

                //construct a policy
                quePolicy.put("Id","s3-sqs-policy-"+suffix);
                JsonObject statement = quePolicy.getJsonArray("Statement").getJsonObject(0);
                statement.put("Sid","sqs-policy-sid-"+suffix);
                JsonObject condition = statement.getJsonObject("Condition");
                JsonObject stringEquals = condition.getJsonObject("StringEquals");
                JsonObject arnEquals = condition.getJsonObject("ArnEquals");
                stringEquals.put("aws:SourceAccount",accountId);
                arnEquals.put("aws:SourceArn",s3Arn);

                sqsArn = AwsHelper.setQueuePolicy(sqsClient,queUrl,quePolicy);

                List<String> events = new ArrayList<>();
                userConfig.getJsonArray("S3_Events").forEach((event)->{
                    events.add(event.toString());
                });
                QueueConfiguration qConfig = QueueConfiguration.builder().queueArn(sqsArn)
                        .id("s3-notification-"+suffix)
                        .eventsWithStrings(events)
                        .build();
                String bucketName = s3Arn.substring(s3Arn.indexOf(":::")+3);
                //System.out.println(bucketName);
                s3.putBucketNotificationConfiguration(PutBucketNotificationConfigurationRequest.builder()
                        .bucket(bucketName)
                        .notificationConfiguration(NotificationConfiguration.builder()
                                .queueConfigurations(qConfig).build())
                        .build());
            }

        }

        s3.close();
        stsClient.close();
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            if(null!=sqsClient) sqsClient.close();
        }));

        while(true){
            List<Message> messages = AwsHelper.receiveLongPollMessages(sqsClient,queUrl,5,5) ;
            //List<Message> messages = receiveMessages(sqsClient, queueUrl);
            for (Message message :
                    messages) {
                //System.out.println(message.body());
                LOGGER.info("[receive S3 events]: "+message.body().toString());
                JsonArray records = new JsonObject(message.body()).getJsonArray("Records");
                if(null!=records){
                    for (int i = 0; i < records.size(); i++) {
                        CloudEvent ce = S3Adapter.adaptS3Event(records.getJsonObject(i));
                        String ret = DeliveryClient.syncDeliverCloudEventRetries(ce,sink,3,2000);
                        //commit event is received by the target
                        if(ret.equals(ce.getSource()+"-"+ce.getId())){
                            AwsHelper.deleteMessage(sqsClient,queUrl,message);
                        }
                    }
                }
            }

            //System.out.println(messages.size());
            try {
                Thread.sleep(5*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

    /**
     * check the vance-sqs exists or not
     * @return vance-sqs queueUrl if it exists
     */
    public static String vanceQueueUrl(SqsClient sqsClient){
        List<String> queueUrls = AwsHelper.listQueues(sqsClient,"vance-s3-sqs");
        if(queueUrls!=null){
            return queueUrls.get(0);
        }
        return null;
    }
}
