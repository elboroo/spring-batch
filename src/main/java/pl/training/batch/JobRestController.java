package pl.training.batch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequiredArgsConstructor
public class JobRestController {

    private final JobLauncher jobLauncher;
    private final Map<String, Job> jobs;
    private final JobExplorer jobExplorer;

    @PostMapping("jobs")
    public ExitStatus runJob(@RequestBody JobDto jobDto) throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        var job = jobs.get(jobDto.getName());
        var jobParameters = new JobParametersBuilder(jobDto.getProperties()).toJobParameters();
        var parameters = new JobParametersBuilder(jobParameters, jobExplorer)
                .getNextJobParameters(job)
                .toJobParameters();
        return jobLauncher.run(job, parameters).getExitStatus();
    }

}
