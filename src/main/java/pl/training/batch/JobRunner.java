package pl.training.batch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Properties;

@Service
@RequiredArgsConstructor
public class JobRunner {

    private final JobLauncher jobLauncher;
    private final Job firstJob;
    private final JobExplorer jobExplorer;

    //@Scheduled(fixedRate = 10_000)
    public void run() throws Exception {
        var properties = new Properties();
        properties.setProperty("fileName", "data.txt");
        var jobParameters = new JobParametersBuilder(properties).toJobParameters();
        var parameters = new JobParametersBuilder(jobParameters, jobExplorer)
                .getNextJobParameters(firstJob)
                .toJobParameters();
        jobLauncher.run(firstJob, parameters);
    }

}
