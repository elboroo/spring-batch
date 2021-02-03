package pl.training.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileUrlResource;

import static pl.training.batch.TransactionMapper.*;

@EnableBatchProcessing
@Configuration
public class BatchConfiguration {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @StepScope
    @Bean
    public ItemStreamReader<Transaction> transactionsFileReader(@Value("#{jobParameters['transactionsFilePath']}") FileUrlResource transactionsFilePath) {
        var transactionMapper = new TransactionMapper();
        return new FlatFileItemReaderBuilder<Transaction>().name("transactionsFileReader")
                .delimited()
                .names(ACCOUNT_NUMBER, TIMESTAMP, AMOUNT)
                .fieldSetMapper(transactionMapper::mapFieldSet)
                .resource(transactionsFilePath)
                .build();
    }

    @StepScope
    @Bean
    public ItemWriter<Transaction> transactionsDatabaseWriter(TransactionsRepository transactionsRepository) {
        var transactionsWriter = new RepositoryItemWriter<Transaction>();
        transactionsWriter.setRepository(transactionsRepository);
        return transactionsWriter;
    }

    @Bean
    public Step importTransactions(ItemReader<Transaction> transactionsFileReader, ItemWriter<Transaction> transactionsDatabaseWriter) {
        return stepBuilderFactory.get("importTransactions")
                .<Transaction, Transaction>chunk(5)
                .reader(transactionsFileReader)
                .writer(transactionsDatabaseWriter)
                .build();
    }

    @Bean
    public Job processAccounts(Step importTransactions) {
        return jobBuilderFactory.get("process")
                .incrementer(new RunIdIncrementer())
                .start(importTransactions)
                .build();
    }

}
