package pl.training.batch;

import lombok.extern.java.Log;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.annotation.AfterJob;
import org.springframework.batch.core.annotation.BeforeJob;

@Log
public class JobExecutionLogger /*implements JobExecutionListener*/ {

    @BeforeJob
    //@Override
    public void beforeJob(JobExecution jobExecution) {
        log.info(String.format("### %s is beginning execution", jobExecution.getJobInstance().getJobName()));
    }

    @AfterJob
    //@Override
    public void afterJob(JobExecution jobExecution) {
        log.info(String.format("### %s has completed execution with status %s", jobExecution.getJobInstance().getJobName(), jobExecution.getStatus()));
    }

}
