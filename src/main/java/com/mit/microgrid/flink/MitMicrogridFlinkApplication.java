package com.mit.microgrid.flink;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableFeignClients(basePackages = { "com.mit.microgrid.*" })
public class MitMicrogridFlinkApplication {

	public static void main(String[] args) {
		SpringApplication.run(MitMicrogridFlinkApplication.class, args);
	}

}
