package pl.training.batch;

import lombok.extern.java.Log;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

@Log
public class SimpleTask implements Tasklet {

    private int counter = 1;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        log.info("Execution no. " + counter);
        return counter++ < 3 ? RepeatStatus.CONTINUABLE : RepeatStatus.FINISHED;
    }

}
