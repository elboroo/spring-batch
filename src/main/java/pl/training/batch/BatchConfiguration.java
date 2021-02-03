package pl.training.batch;

import lombok.extern.java.Log;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.core.step.tasklet.CallableTaskletAdapter;
import org.springframework.batch.core.step.tasklet.MethodInvokingTaskletAdapter;
import org.springframework.batch.core.step.tasklet.SystemCommandTasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.repeat.policy.CompositeCompletionPolicy;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.batch.repeat.policy.TimeoutTerminationPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Log
@EnableBatchProcessing
@Configuration
public class BatchConfiguration {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @StepScope
    @Bean
    public SimpleTask simpleTask(@Value("#{jobParameters['fileName']}") String fileName) {
        return new SimpleTask(fileName);
    }

    @Bean
    public StepExecutionListener promotionListener() {
        var listener = new ExecutionContextPromotionListener();
        listener.setKeys(new String[] {"counter"});
        return listener;
    }

    @Bean
    public Step firstStep(SimpleTask simpleTask) {
        return stepBuilderFactory.get("firstStep")
                .tasklet(simpleTask)
                .listener(promotionListener())
                .build();
    }

    @Bean
    public MethodInvokingTaskletAdapter methodInvokingTaskletAdapter(AccountService accountService) {
        var adapter = new MethodInvokingTaskletAdapter();
        adapter.setTargetObject(accountService);
        adapter.setTargetMethod("incrementBalance");
        adapter.setArguments(new Object[] { 20L });
        return adapter;
    }

    @Bean
    public CallableTaskletAdapter callableTaskletAdapter() {
        var adapter = new CallableTaskletAdapter();
        adapter.setCallable(() -> {
            log.info("Executing background task...");
            return RepeatStatus.FINISHED;
        });
        return adapter;
    }

    @Bean
    public SystemCommandTasklet systemCommandTasklet() {
        var adapter = new SystemCommandTasklet();
        adapter.setCommand("ls");
        adapter.setTimeout(2_000);
        adapter.setWorkingDirectory("/Users/lukas");
        adapter.setEnvironmentParams(new String[] { "JAVA_HOME=/java" });
        adapter.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return adapter;
    }

    @Bean
    public Step secondStep(MethodInvokingTaskletAdapter methodInvokingTaskletAdapter) {
        return stepBuilderFactory.get("secondStep")
                .tasklet(methodInvokingTaskletAdapter)
                .build();
    }

    @Bean
    public ItemReader<String> itemReader() {
        var items = new ArrayList<String>(200);
        for (int i = 0; i < 200; i++) {
            items.add(UUID.randomUUID().toString());
        }
        return new ListItemReader<>(items);
    }

    @Bean
    public ItemWriter<String> itemWriter() {
        return items -> {
            log.info("### New chunk");
            items.forEach(System.out::println);
        };
    }

    @Bean
    public CompletionPolicy completionPolicy() {
        var policy = new CompositeCompletionPolicy();
        policy.setPolicies(new CompletionPolicy[] { new TimeoutTerminationPolicy(1_000), new SimpleCompletionPolicy(100)});
        return policy;
    }

    @Bean
    public Step thirdStep() {
        return stepBuilderFactory.get("thirdStep")
                //.<String, String>chunk(100)
                .<String, String>chunk(completionPolicy())
                .reader(itemReader())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public DefaultJobParametersValidator defaultJobParametersValidator() {
        var validator = new DefaultJobParametersValidator();
        validator.setRequiredKeys(new String[] {"fileName"});
        validator.setOptionalKeys(new String[] {"run.id", "executionDate"});
        return validator;
    }

    @Bean
    public CompositeJobParametersValidator validators() {
        var validator = new CompositeJobParametersValidator();
        validator.setValidators(List.of(defaultJobParametersValidator(), new FileNameValidator()));
        return validator;
    }

    @Bean
    public Flow processingFlow(Step firstStep, Step secondStep, Step thirdStep) {
        return new FlowBuilder<Flow>("processingFlow")
                .start(firstStep)
                .next(secondStep)
                .next(thirdStep)
                .build();
    }

    @Bean
    public Step fourthStep(Flow processingFlow, JobExplorer jobExplorer) {
        return stepBuilderFactory.get("fourthStep")
                //.job(custom_custom)
                .tasklet(new JobStatusExplorer(jobExplorer))
                //.flow(processingFlow)
                .build();
    }

    @Bean
    public Job firstJob(Step fourthStep, Flow processingFlow) {
        return jobBuilderFactory.get("firstJob2")
                .validator(validators())
                //.incrementer(new RunIdIncrementer())
                .incrementer(new ExecutionDateIncrementer())
                .listener(new JobExecutionLogger())
                .start(fourthStep)
                //.next(new CustomDecider())
                //.on(ExitStatus.FAILED.getExitDescription()).to(processingFlow)
                //.next(someStep)
                .build();

                //.start(fourthStep)
                //.build();
    }


}
