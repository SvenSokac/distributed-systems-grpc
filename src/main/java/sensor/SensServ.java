package sensor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;
import java.io.IOException;
import java.net.InetAddress;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
public class SensServ {

    public static void main(String[] args) throws IOException, InterruptedException {
        //jmDNS registration
        registerService("SensorService", 50056, "_sensor._tcp.local.", "Streaming sensor data");

        //start of gRPC server
        Server server = ServerBuilder.forPort(50056)
                .addService(new SensorServiceImpl())
                .build();
       
        System.out.println("SensorService has started on port 50056");
        //starting server
        server.start();
        server.awaitTermination();
    }
    //registers service on network
     private static void registerService(String name, int port, String type, String description) {
        try {
            JmDNS jmdns = JmDNS.create(InetAddress.getLocalHost());
            ServiceInfo serviceInfo = ServiceInfo.create(type, name, port, description);
            jmdns.registerService(serviceInfo);
            System.out.println(name + " registered with jmDNS, on port " + port);
        } catch (Exception e) {
        	//this handles registration error
            System.err.println("registration failed: " + e.getMessage());
        }
    }
    //sensorserviceimpl simulates streaming of sensor data
    static class SensorServiceImpl extends SensorServiceGrpc.SensorServiceImplBase {
        @Override
        public void sensorStreamData(SensorRequest request, StreamObserver<SensorData> responseObserver) {
            Random random = new Random();
            //adding timestamp
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
            //simulates sending 10 sensor data
            for (int i = 0; i < 10; i++) {
                SensorData data = SensorData.newBuilder()
                        .setTemperature(20 + random.nextDouble() * 10) //random temperature from 20 to 30
                        .setPressure(1000 + random.nextDouble() * 50) //pressure is from 1000 to 1050, 
                        .setVibration(random.nextDouble() * 6)//vibration I set from 0 to 6
                        .setTimestamp(LocalDateTime.now().format(formatter))
                        .build();
                //this sends sensor data to client
                responseObserver.onNext(data);

                try {
                	//delay of 1second
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //signals that service streaming is complete
            responseObserver.onCompleted();
        }
    }
}