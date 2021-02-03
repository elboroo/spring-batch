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
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileUrlResource;

import javax.sql.DataSource;

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
    public ItemReader<Account> accountsDatabaseReader(DataSource dataSource) {
        var accountMapper = new AccountMapper();
        return new JdbcCursorItemReaderBuilder<Account>().name("accountsDatabaseReader")
                .dataSource(dataSource)
                .sql("select * from accounts order by account_number")
                .rowMapper(accountMapper::mapRow)
                .build();
    }

    @Bean
    public TransactionsProcessor transactionsProcessor(TransactionsRepository transactionsRepository) {
        return new TransactionsProcessor(transactionsRepository);
    }

    @Bean
    public ItemWriter<Account> accountsDatabaseWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Account>()
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("update accounts set balance = :balance where account_number = :number")
                .build();
    }

    @Bean
    public Step applyTransactions(ItemReader<Account> accountsDatabaseReader, TransactionsProcessor transactionsProcessor, ItemWriter<Account> accountsDatabaseWriter) {
        return stepBuilderFactory.get("applyTransactions")
                .<Account, Account>chunk(200)
                .reader(accountsDatabaseReader)
                .processor(transactionsProcessor)
                .writer(accountsDatabaseWriter)
                .build();
    }

    @Bean
    public Job processAccounts(Step importTransactions, Step applyTransactions) {
        return jobBuilderFactory.get("process")
                .incrementer(new RunIdIncrementer())
                .start(importTransactions)
                .next(applyTransactions)
                .build();
    }

}
