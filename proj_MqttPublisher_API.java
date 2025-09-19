package MqttPublisher_API;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class proj_MqttPublisher_API implements MqttCallback {
	
	    static MqttClient sampleClient;

	    // 토픽과 필드 정보를 담는 클래스
	    static class TopicInfo {
	        String topic;
	        String jsonField;
	        public TopicInfo(String topic, String jsonField) {
	            this.topic = topic;
	            this.jsonField = jsonField;
	        }
	    }

	    // 토픽 배열 선언
	    TopicInfo[] ObsGrp  = {
	            new TopicInfo("weather/observation/temp", "tmp"),
	            new TopicInfo("weather/observation/humi", "humi"),
	            new TopicInfo("weather/observation/rainfall", "rainf"),
	            new TopicInfo("weather/observation/precipType", "pre")
	    };

	    public static void main(String[] args) {
	        proj_MqttPublisher_API obj = new proj_MqttPublisher_API();
	        obj.run();
	    }

	    public void run() {
	        connectBroker();

	        try {
	            sampleClient.subscribe("weather/actuator/led");
	        } catch (MqttException e1) {
	            e1.printStackTrace();
	        }

	        while (true) {
	            try {
	                String[] weather_data = get_weather_data();

	                publishGroup(ObsGrp, weather_data);

	                Thread.sleep(5000);

	            } catch (Exception e) {
	                try {
	                    sampleClient.disconnect();
	                } catch (MqttException e1) {
	                    e1.printStackTrace();
	                }
	                e.printStackTrace();
	                System.out.println("Disconnected");
	                System.exit(0);
	            }
	        }
	    }
	    
	    public void publishGroup(TopicInfo[] group, String[] data) {
	    	 StringBuilder jsonBuilder = new StringBuilder("{");
	    	    for (int i = 0; i < group.length; i++) {
	    	        jsonBuilder.append(String.format("\"%s\": %s", group[i].jsonField, data[i]));
	    	        if (i < group.length - 1) jsonBuilder.append(", ");
	    	    }
	    	    jsonBuilder.append("}");
	    	    
	    	    String combinedTopic = "weather/observation";
	    	    publish_data(combinedTopic, jsonBuilder.toString());
	    }
	    
	    public void connectBroker() {
	        String broker = "tcp://127.0.0.1:1883";
	        String clientId = "practice";
	        MemoryPersistence persistence = new MemoryPersistence();
	        try {
	            sampleClient = new MqttClient(broker, clientId, persistence);
	            MqttConnectOptions connOpts = new MqttConnectOptions();
	            connOpts.setCleanSession(true);
	            System.out.println("Connecting to broker: " + broker);
	            sampleClient.connect(connOpts);
	            sampleClient.setCallback(this);
	            System.out.println("Connected");
	        } catch (MqttException me) {
	            System.out.println("reason " + me.getReasonCode());
	            System.out.println("msg " + me.getMessage());
	            System.out.println("loc " + me.getLocalizedMessage());
	            System.out.println("cause " + me.getCause());
	            System.out.println("excep " + me);
	            me.printStackTrace();
	        }
	    }

	    public void publish_data(String topic_input, String data) {
	        int qos = 0;
	        try {
	            System.out.println("Publishing message to " + topic_input + ": " + data);
	            sampleClient.publish(topic_input, data.getBytes(), qos, false);
	            System.out.println("Message published");
	        } catch (MqttException me) {
	            System.out.println("reason " + me.getReasonCode());
	            System.out.println("msg " + me.getMessage());
	            System.out.println("loc " + me.getLocalizedMessage());
	            System.out.println("cause " + me.getCause());
	            System.out.println("excep " + me);
	            me.printStackTrace();
	        }
	    }

	    public String[] get_weather_data() {
	        Date current = new Date(System.currentTimeMillis());
	        SimpleDateFormat d_format = new SimpleDateFormat("yyyyMMddHHmmss");
	        String date = d_format.format(current).substring(0, 8);
	        String time = d_format.format(current).substring(8, 10);

	        String base_time;
	        int hour = Integer.parseInt(time);
	        if (hour % 3 == 0) {
	            base_time = String.format("%02d00", hour);
	        } else {
	            base_time = String.format("%02d00", (hour / 3) * 3);
	        }

	        String url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"
	                + "?serviceKey=SNglu6iHPdeaFM13SJuE4DsNL%2FXQoVyczjXdKSoThh0SQ8w2yoDGTgOS2syutF2Yja%2F3dIDLhAe2b19TRv9ACg%3D%3D"
	                + "&pageNo=1&numOfRows=1000"
	                + "&dataType=XML"
	                + "&base_date=" + date
	                + "&base_time=" + base_time
	                + "&nx=55"
	                + "&ny=127";

	        String temp = "";
	        String humi = "";
	        String rainf = "";
	        String pre = "";
	        Document doc = null;

	        try {
	            doc = Jsoup.connect(url).get();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }

	        if (doc != null) {
	            Elements elements = doc.select("item");

	            for (Element e : elements) {
	                String category = e.select("category").text();
	                String obsrValue = e.select("obsrValue").text();

	                if (category.equals("T1H")) {
	                    temp = obsrValue;
	                }
	                if (category.equals("REH")) {
	                    humi = obsrValue;
	                }
	                if (category.equals("RN1")) {
	                    rainf = obsrValue;
	                }
	                if (category.equals("PTY")) {
	                    pre = obsrValue;
	                }
	            }
	        }

	        return new String[]{temp, humi, rainf, pre};
	    }

	    @Override
	    public void connectionLost(Throwable cause) {
	        System.out.println("Connection lost");
	    }

	    @Override
	    public void deliveryComplete(IMqttDeliveryToken token) {}

	    @Override
	    public void messageArrived(String topic, MqttMessage msg) throws Exception {
	        if (topic.equals("weather/actuator/led")) {
	            System.out.println("--------------------Actuator Function--------------------");
	            System.out.println("LED Display changed");
	            System.out.println("LED: " + msg.toString());
	            System.out.println("---------------------------------------------------------");
	        }
	    }
	}
