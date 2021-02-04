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
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
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
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Log
@EnableScheduling
@EnableBatchProcessing
@Configuration
public class BatchConfiguration {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Bean
    public ItemReader<String> itemReader() {
        var items = new ArrayList<String>(20);
        for (int i = 0; i < 20; i++) {
            var name = i < 10 ? "#1#" : "#2#";
            items.add(name + UUID.randomUUID().toString());
        }
        return new ListItemReader<>(items);
    }

    @Bean
    public ItemWriter<String> itemWriter() {
        return items -> {
            log.info("### New chunk");
            items.forEach(entry -> {
                System.out.println(entry + " " + Thread.currentThread().getName());
            });
        };
    }

    @Bean
    public Step step() {
        return stepBuilderFactory.get("step")
                .<String, Future<String>>chunk(10)
                .reader(itemReader())
                .processor(asyncItemProcessor())
                .writer(asyncItemWriter())
                //.taskExecutor(new ConcurrentTaskExecutor())
                .build();
    }

    @Bean
    public ItemProcessor<String, String> processor() {
        var random = new Random();
        return text -> {
            var value = random.nextInt(text.startsWith("#1") ? 5_000: 10);
            System.out.println(value);
            Thread.sleep(value);
            return text;
        };
    }

    @Bean
    public AsyncItemProcessor<String, String> asyncItemProcessor() {
        var asyncProcessor = new AsyncItemProcessor<String, String>();
        asyncProcessor.setDelegate(processor());
        asyncProcessor.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return asyncProcessor;
    }

    @Bean
    public AsyncItemWriter<String> asyncItemWriter() {
        var asyncItemWriter = new AsyncItemWriter<String>();
        asyncItemWriter.setDelegate(itemWriter());
        return asyncItemWriter;
    }

    @Bean
    public Step step2() {
        return stepBuilderFactory.get("step2")
                .<String, String>chunk(10)
                .reader(itemReader())
                .writer(itemWriter())
                //.taskExecutor(new ConcurrentTaskExecutor())
                .build();
    }

    @Bean
    public Job firstJob(Step step, Step step2) {
        var step2Flow = new FlowBuilder<Flow>("step2Flow")
                .start(step2)
                .build();

        var flow = new FlowBuilder<Flow>("flow")
                .start(step)
                .split(new SimpleAsyncTaskExecutor())
                .add(step2Flow)
                .build();

        return jobBuilderFactory.get("firstJob2")
                .incrementer(new ExecutionDateIncrementer())
                .start(step)
                //.end()
                .build();
    }

}
