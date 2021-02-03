package pl.training.batch;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

@Log
@RequiredArgsConstructor
public class SimpleTask implements Tasklet {

    private int counter = 1;

    private final String fileName;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        /*var parameters = chunkContext.getStepContext().getJobParameters();
        log.info("Found parameter fileName: " + parameters.get("fileName"));*/
        log.info("Found parameter fileName: " + fileName);
        log.info("Execution no. " + counter);
        return counter++ < 3 ? RepeatStatus.CONTINUABLE : RepeatStatus.FINISHED;
    }

}
