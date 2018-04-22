package com.tasks.csvsender;

import com.tasks.csvsender.Controller.ParserController;
import com.tasks.csvsender.Controller.RabbitController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@PropertySource("classpath:application.properties")
public class CsvsenderApplication implements CommandLineRunner {

	@Autowired
	ParserController parser;

	@Autowired
	RabbitController rabbitController;

	@Value("#{'${parser.paths}'.split(',')}")
	private List<String> propertiesList;

	@Value("${sender.interval.seconds}")
	private int interval;

	@Value("${app.core.pool.size}")
	private int poolSize;

	@Override
	public void run(String... args) throws Exception {
		ScheduledExecutorService executorService = Executors.newScheduledThreadPool(poolSize);
		rabbitController.setChannel();
		Runnable runnable = new Runnable() {
			public void run() {
				System.out.println("starting");
				long startTime = System.nanoTime();
                List<File> files = parser.getFiles(propertiesList);
				try {
						for (File file : files) {
							parser.sendFile(file);
						}

				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					rabbitController.closeChannel();
					rabbitController.closeConnection();
					long endTime = System.nanoTime();
					System.out.println((endTime - startTime)/1000000 + "ms");
					}
				for (File file : files) {
					boolean success = file.renameTo(new File(file.getAbsolutePath().substring(0, file.getAbsolutePath().length() - 4)));
					System.out.println(success);

				}
            }

		};

		executorService.scheduleAtFixedRate(runnable, 0, interval , TimeUnit.SECONDS);

	}



	public static void main(String[] args) {
		SpringApplication.run(CsvsenderApplication.class, args);
	}

}
