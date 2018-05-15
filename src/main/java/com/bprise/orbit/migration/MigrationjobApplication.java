package com.bprise.orbit.migration;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.bprise.orbit.migration.task.MigrationTask;

@ComponentScan("com.bprise.orbit.migration")
@SpringBootApplication
public class MigrationjobApplication implements CommandLineRunner {
	@Autowired
	private MigrationTask migrationTask;
	public static void main(String[] args) {
		SpringApplication.run(MigrationjobApplication.class, args);
	}

	@Override
	public void run(String... arg0) throws Exception {
		/*LOGGER.info("Migration job has started");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		
		Date date = formatter.parse("2018-02-10T13:16:18.131");
		final long nextUpdateTime = date.getTime();*/
		final long nextUpdateTime = new Date().getTime();
		migrationTask.mainTask(nextUpdateTime);
		System.exit(0);
	}
	
	
}
