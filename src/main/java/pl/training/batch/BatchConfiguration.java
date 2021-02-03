package pl.training.batch;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileUrlResource;

@EnableBatchProcessing
@Configuration
public class BatchConfiguration {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Bean
    public ItemReader<Transaction> transactionsFileReader(@Value("#{jobParameters['transactionsFilePath']}") FileUrlResource transactionsFilePath) {
        return new FlatFileItemReaderBuilder<>().name("transactionsFileReader")
                .delimited()
                .names(new String[] { "accountNumber", "timestamp", "amount" })
                .fieldSetMapper()

    }

    @Bean
    public Step importTransactions() {
        return stepBuilderFactory.get("importTransactions")
                .<String, String>chunk(5)
                .reader()
                .writer()
                .build();
    }

}
