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
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.PassThroughFieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
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

   /* @StepScope
    @Bean
    public ItemStreamReader<Transaction> transactionsFileReader(@Value("#{jobParameters['transactionsFilePath']}") FileUrlResource transactionsFilePath) {
        return new FlatFileItemReaderBuilder<Transaction>().name("transactionsFileReader")
                .delimited()
                .names(ACCOUNT_NUMBER, TIMESTAMP, AMOUNT)
                .fieldSetMapper(new TransactionMapper())
                .resource(transactionsFilePath)
                .build();
    }*/

    @StepScope
    @Bean
    public ItemStreamReader<FieldSet> transactionsFileReader(@Value("#{jobParameters['transactionsFilePath']}") FileUrlResource transactionsFilePath) {
        return new FlatFileItemReaderBuilder<FieldSet>().name("transactionsFileReader")
                .lineTokenizer(new DelimitedLineTokenizer())
                .fieldSetMapper(new PassThroughFieldSetMapper())
                .resource(transactionsFilePath)
                .build();
    }

    @StepScope
    @Bean
    public SafeTransactionReader safeTransactionReader(ItemStreamReader<FieldSet> transactionsFileReader){
        return new SafeTransactionReader(transactionsFileReader);
    }

    @StepScope
    @Bean
    public ItemWriter<Transaction> transactionsDatabaseWriter(TransactionsRepository transactionsRepository) {
        var transactionsWriter = new RepositoryItemWriter<Transaction>();
        transactionsWriter.setRepository(transactionsRepository);
        return transactionsWriter;
    }

    @Bean
    public Step importTransactions(SafeTransactionReader safeTransactionReader /*ItemReader<Transaction> transactionsFileReader*/, ItemWriter<Transaction> transactionsDatabaseWriter) {
        return stepBuilderFactory.get("importTransactions")
                .<Transaction, Transaction>chunk(5)
                .reader(safeTransactionReader)
                .writer(transactionsDatabaseWriter)
                .faultTolerant()
                .build();
    }

    @Bean
    public ItemReader<Account> accountsDatabaseReader(DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<Account>().name("accountsDatabaseReader")
                .dataSource(dataSource)
                .sql("select * from accounts order by account_number")
                .rowMapper(new AccountMapper())
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

    @StepScope
    @Bean
    public FlatFileItemWriter<Account> accountsFileWriter(@Value("#{jobParameters['summaryFilePath']}") FileUrlResource summaryFile) {
        var lineAggregator = new DelimitedLineAggregator<Account>();
        var fieldExtractor = new BeanWrapperFieldExtractor<Account>();
        fieldExtractor.setNames(new String[]{ "number", "balance" });
        fieldExtractor.afterPropertiesSet();
        lineAggregator.setFieldExtractor(fieldExtractor);
        return new FlatFileItemWriterBuilder<Account>().name("accountsFileWriter")
                .resource(summaryFile)
                .lineAggregator(lineAggregator)
                .build();
    }

    @Bean
    public Step generateSummary(ItemReader<Account> accountsDatabaseReader, ItemWriter<Account> accountsFileWriter) {
        return stepBuilderFactory.get("generateSummary")
                .<Account, Account>chunk(100)
                .reader(accountsDatabaseReader)
                .writer(accountsFileWriter)
                .build();
    }

    @Bean
    public Job processAccounts(Step importTransactions, Step applyTransactions, Step generateSummary) {
        return jobBuilderFactory.get("process")
                .incrementer(new RunIdIncrementer())
                .start(importTransactions)
                .next(applyTransactions)
                .next(generateSummary)
                .build();
    }

}
