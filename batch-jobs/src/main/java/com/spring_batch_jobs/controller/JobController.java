package com.spring_batch_jobs.controller;

import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/jobs")
public class JobController {
    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    private Job job;

    @PostMapping("/importCustomers/{fileName}")
    public String jobLaunch( @PathVariable("fileName") final String fileName ) {

        final JobParameters parameters = new JobParametersBuilder().addString("fileName",fileName)
                .addLong("startAt",System.currentTimeMillis()).toJobParameters();

        try{
            JobExecution execution = jobLauncher.run(job, parameters);
            return execution.getStatus().toString();
        } catch (JobInstanceAlreadyCompleteException | JobExecutionAlreadyRunningException |
                 JobParametersInvalidException | JobRestartException e) {
            e.printStackTrace();
            return "Failed to launch job" + e.getMessage();

        }

    }

}

