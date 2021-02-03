package pl.training.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

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
    public Step firstStep(SimpleTask simpleTask) {
        return stepBuilderFactory.get("firstStep")
                .tasklet(simpleTask)
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
    public Job job(Step firstStep) {
        return jobBuilderFactory.get("firstJob")
                .validator(validators())
                //.incrementer(new RunIdIncrementer())
                .incrementer(new ExecutionDateIncrementer())
                .start(firstStep)
                .build();
    }

}
