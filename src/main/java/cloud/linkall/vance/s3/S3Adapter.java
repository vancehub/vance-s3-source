package cloud.linkall.vance.s3;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.types.Time;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.net.URI;
import java.time.OffsetDateTime;

public class S3Adapter {
    private static CloudEventBuilder template = CloudEventBuilder.v1();;

    public static CloudEvent adaptS3Event(JsonObject record){

        JsonObject responseElements = record.getJsonObject("responseElements");
        template.withId(responseElements.getString("x-amz-request-id")+"."+responseElements.getString("x-amz-id-2"));
        URI uri = URI.create(record.getString("eventSource")+"."+record.getString("awsRegion")+"."+
                record.getJsonObject("s3").getJsonObject("bucket").getString("name"));
        template.withSource(uri);
        template.withType("com.amazonaws.s3."+record.getString("eventName"));
        template.withDataContentType("application/json");
        String timeStr = record.getString("eventTime");
        template.withTime(Time.parseTime("time", timeStr));
        template.withData(record.getJsonObject("s3").toBuffer().getBytes());

        return template.build();

    }
}
