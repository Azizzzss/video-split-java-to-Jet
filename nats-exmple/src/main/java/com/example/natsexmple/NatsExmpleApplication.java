package com.example.natsexmple;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.JsonUtils;
import org.springframework.boot.CommandLineRunner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

@SpringBootApplication
public class NatsExmpleApplication {

	public static void main(String[] args) {
		SpringApplication.run(NatsExmpleApplication.class, args);
	}



	@Bean
	CommandLineRunner commandLineRunner(){
		return args -> {

			try {
			//Obj to create the connection with the nats serv
			Connection nc  = Nats.connect("nats://localhost:4222");
			// obj to use it later for all info off our stream
			JetStreamManagement jsm = nc .jetStreamManagement();
			// here the buddy's configuration get ready
			StreamConfiguration streamConfig = StreamConfiguration.builder()
					.name("z")
					.subjects("v")
					.storageType(StorageType.Memory)
					.build();
			// here we set an object of a class that allow us to throw in it the config obj  and return info
			StreamInfo streamInfo = jsm.addStream(streamConfig);
			/// here just to make things more clear
			JsonUtils.printFormatted(streamInfo);

			// the budy is ready and we lunch the stream here
			JetStream js = nc .jetStream();
				String videoPath = "C:\\Users\\Aziz\\Downloads\\WhatsApp Video 2021-10-22 at 01.44.23.mp4";

				// Create file input stream
				FileInputStream fis = new FileInputStream(videoPath);

				// Define buffer size
				int bufferSize = 1024 * 1024; // 1 MB

				// Create byte array buffer
				byte[] buffer = new byte[bufferSize];

				// Define chunk size
				int chunkSize = 1024 * 1024; // => 1 MB



				// Read and publish video chunks
				int bytesRead;
				int numB = 0 ;

				while ((bytesRead = fis.read(buffer, 0, chunkSize)) > 0) {
					// Create message
					Message msg = NatsMessage.builder()
							.subject("v")
							.data(buffer)
							.build();

					// Publish message to JetStream
					js.publish(msg);

					// Clear buffer
					buffer = new byte[bufferSize];

					numB++;
					System.out.println("number of buffers :  " + numB );
				}

				fis.close();
				String outputPath = "C:\\Users\\Aziz\\Desktop\\videoreciezved\\Video1_out.mp4";


				FileOutputStream fos = new FileOutputStream(outputPath);
				JetStreamSubscription s = js.subscribe("v");
				int numBr = 0 ;

				while (true){
					Message m = s.nextMessage(Duration.ofSeconds(5));

					if (m == null) {
						break;
					}
					numBr++;

					System.out.println("number of buffers recieved :  " + numBr );
					fos.write(m.getData());
				}




			}catch (InterruptedException | IllegalStateException e) {
				System.out.println("Error connecting to NATS server: " + e.getMessage());
			} catch (JetStreamApiException e) {
				System.out.println("Error creating JetStream stream: " + e.getMessage());
			} catch (IOException e ) {
				System.out.println("Failed to read video file: " + e.getMessage());
			}catch (Exception e) {
				System.out.println("Unexpected error: " + e.getMessage());
			}


		};
	}

}
