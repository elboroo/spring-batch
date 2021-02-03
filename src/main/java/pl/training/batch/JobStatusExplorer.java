package pl.training.batch;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

@Log
@RequiredArgsConstructor
public class JobStatusExplorer implements Tasklet {

    private final JobExplorer jobExplorer;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        var jobName = chunkContext.getStepContext().getJobName();
        var jobInstance = jobExplorer.getJobInstances(jobName, 0, Integer.MAX_VALUE);
        log.info(String.format("%s job instances: ", jobName, jobInstance.size()));
        jobInstance.forEach(instance -> {
            log.info(jobExplorer.getJobExecutions(instance).toString());
        });
        return RepeatStatus.FINISHED;
    }

}
