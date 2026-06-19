package sensor;
import analysis.AnalysisResult;
import analysis.AnalysisServiceGrpc;
import analysis.SensorData;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;
import javax.swing.JOptionPane;
import java.net.InetAddress;
public class SensCli {
	//we use to discover server with jmDNS service
    private static ServiceInfo discoverService(String type) {
        try {
            JmDNS jmdns = JmDNS.create(InetAddress.getLocalHost());
            ServiceInfo[] infos = jmdns.list(type);
            if (infos.length > 0) {
                System.out.println("Discovered service: " + infos[0].getName());
                return infos[0];
            } else {
                System.out.println("No service found, for type: " + type);
            }
        } catch (Exception e) {
            System.err.println("Discovery failed: " + e.getMessage());
        }
        return null;
    }

    public static void main(String[] args) {
        //discover for SensorService
        ServiceInfo sensorInfo = discoverService("_sensor._tcp.local.");
        ManagedChannel sensorChannel = ManagedChannelBuilder.forAddress(sensorInfo.getHostAddresses()[0], sensorInfo.getPort())
                .usePlaintext()
                .build();
        SensorServiceGrpc.SensorServiceStub sensorStub = SensorServiceGrpc.newStub(sensorChannel);

        //discover for AnalysisService
        ServiceInfo analysisInfo = discoverService("_analysis._tcp.local.");
        ManagedChannel analysisChannel = ManagedChannelBuilder.forAddress(analysisInfo.getHostAddresses()[0], analysisInfo.getPort())
                .usePlaintext()
                .build();
        AnalysisServiceGrpc.AnalysisServiceBlockingStub analysisStub = AnalysisServiceGrpc.newBlockingStub(analysisChannel);

        sensor.SensorRequest request = sensor.SensorRequest.newBuilder()
                .setBridgeId(1)
                .build();

        //sensor data forwarded to analysis
        sensorStub.sensorStreamData(request, new StreamObserver<sensor.SensorData>() {
            @Override
            public void onNext(sensor.SensorData data) {
                System.out.println("Sensor data:");
                System.out.println("Temperature is: " + data.getTemperature());
                System.out.println("Pressure is: " + data.getPressure());
                System.out.println("Vibration is: " + data.getVibration());
                System.out.println("Timestamp: " + data.getTimestamp());

                //converted to analysis.SensorData
                SensorData analysisData = SensorData.newBuilder()
                        .setTemperature(data.getTemperature())
                        .setPressure(data.getPressure())
                        .setVibration(data.getVibration())
                        .setTimestamp(data.getTimestamp())
                        .build();

                //sending to AnalysisService
                AnalysisResult result = analysisStub.analyzeSensorData(analysisData);

                System.out.println("Analysis result:");
                System.out.println("Status: " + result.getStatus());
                System.out.println("Message: " + result.getMessage());
                System.out.println("-----------------------"); //added line breaker for better visual

                //popup window
                String sensorInfo = "Temperature: " + data.getTemperature() +
                        "\nPressure: " + data.getPressure() +
                        "\nVibration: " + data.getVibration() +
                        "\nTimestamp: " + data.getTimestamp();

                String analysisInfo = "Status: " + result.getStatus() +
                        "\nMessage: " + result.getMessage();

                JOptionPane.showMessageDialog(null,
                        "Sensor Data:\n" + sensorInfo + "\n\nAnalysis Result:\n" + analysisInfo,
                        "Sensor Report",
                        JOptionPane.INFORMATION_MESSAGE);
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Error when receiving data: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
            	//for streaming completion and shutdown channel
                System.out.println("Stream is completed.");
                sensorChannel.shutdown();
                analysisChannel.shutdown();
            }
        });

        //this will keep the client alive when the stream runs 
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}