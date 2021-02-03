package pl.training.batch;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

import java.util.Random;

public class CustomDecider implements JobExecutionDecider {

    private final Random random = new Random();

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
        if (random.nextBoolean()) {
            return FlowExecutionStatus.COMPLETED;
        } else {
            return FlowExecutionStatus.FAILED;
        }
    }

}
