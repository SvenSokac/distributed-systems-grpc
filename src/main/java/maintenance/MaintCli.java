package maintenance;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.List;
public class MaintCli {

    public static void main(String[] args) {
    	//create a gRPC channel to connect to the server on port 50058 
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50058)
                .usePlaintext()
                .build();
        //this is an asynchronous stub for maintenanceservice
        MaintenanceServiceGrpc.MaintenanceServiceStub stub = MaintenanceServiceGrpc.newStub(channel);
        //creating streamobserver for recieving  responses form server
        StreamObserver<MaintenanceRequest> requestObserver = stub.manageReports(new StreamObserver<MaintenanceResponse>() {
            @Override
            public void onNext(MaintenanceResponse response) {
                System.out.println("Received respons:");
                System.out.println("Action: " + response.getAction());
                System.out.println("Note: " + response.getTechnicianNote());
                System.out.println("-----------------"); //added line breaker for better visual
            }

            @Override
            public void onError(Throwable t) {
            	 //for errors in the communication
                System.err.println("Error in response: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
            	//this is  when the server has finished sending response
                System.out.println("Report stream done.");
                channel.shutdown();
            }
        });

        //sending multiple reports at once
        List<MaintenanceRequest> reports = Arrays.asList(
                MaintenanceRequest.newBuilder()
                        .setReportId("1")
                        .setIssueSummary("Vibration is to high")
                        .setLocation("Bridge")
                        .build(),
                MaintenanceRequest.newBuilder()
                        .setReportId("2")
                        .setIssueSummary("Temperature is too high")
                        .setLocation("Road")
                        .build(),
                MaintenanceRequest.newBuilder()
                        .setReportId("3")
                        .setIssueSummary("Pressure is to high")
                        .setLocation("Road")
                        .build()
        );
        //sending reports to server with 1second delay
        for (MaintenanceRequest report : reports) {
            requestObserver.onNext(report);
            try {
                Thread.sleep(1000); // this is delay between reports so they dont overlap
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //telling server that we have finished sending reports
        requestObserver.onCompleted();
    }
}
