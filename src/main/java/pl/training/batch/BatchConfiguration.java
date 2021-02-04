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
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.mapping.PassThroughFieldSetMapper;
import org.springframework.batch.item.file.mapping.PatternMatchingCompositeLineMapper;
import org.springframework.batch.item.file.transform.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileUrlResource;

import javax.sql.DataSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                .<Transaction, Transaction>chunk(1)
                .reader(safeTransactionReader)
                .writer(transactionsDatabaseWriter)
                .faultTolerant()
                .startLimit(3)
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

    @StepScope
    @Bean
    public FlatFileItemReader<Customer> customersFileReader(@Value("#{jobParameters['customersFilePath']}") FileUrlResource customersFile) {
        return new FlatFileItemReaderBuilder<Customer>().name("customersFileReader")
                .resource(customersFile)
                .fixedLength()
                .columns(new Range[] { new Range(1, 5), new Range(6, 15), new Range(16) })
                .names("firstName", "lastName", "address")
                .targetType(Customer.class)
                .build();
    }

    @Bean
    public ItemWriter<Customer> customerConsoleWriter() {
        return list -> list.forEach(System.out::println);
    }

    @Bean
    public Step importCustomers(FlatFileItemReader<Customer> customersCompositeFileReader /*FlatFileItemReader<Customer> customersFileReader*/, ItemWriter<Customer> customerConsoleWriter) {
        return stepBuilderFactory.get("importCustomers")
                .<Customer, Customer>chunk(100)
                .reader(customersCompositeFileReader)
                .writer(customerConsoleWriter)
                .build();
    }

    @Bean
    public DelimitedLineTokenizer customerTokenizer() {
        var tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("prefix","firstName", "lastName", "address");
        return tokenizer;
    }

    @Bean
    public DelimitedLineTokenizer transactionTokenizer() {
        var tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("prefix", "number", "timestamp", "amount");
        return tokenizer;
    }

    @Bean
    public PatternMatchingCompositeLineMapper lineMapper() {
        var lineTokenizers = new HashMap<String, LineTokenizer>();
        lineTokenizers.put("c*", customerTokenizer());
        lineTokenizers.put("t*", transactionTokenizer());

        Map<String, FieldSetMapper> mappers = new HashMap<>();
        var customerMapper = new BeanWrapperFieldSetMapper<Customer>();
        customerMapper.setTargetType(Customer.class);
        customerMapper.setStrict(false);
        mappers.put("c*", customerMapper);
        mappers.put("t*", new TransactionMapper());

        PatternMatchingCompositeLineMapper patterMatchingMapper = new PatternMatchingCompositeLineMapper();
        patterMatchingMapper.setTokenizers(lineTokenizers);
        patterMatchingMapper.setFieldSetMappers(mappers);
        return patterMatchingMapper;
    }

    @StepScope
    @Bean
    public FlatFileItemReader<Customer> customersCompositeFileReader(@Value("#{jobParameters['customersFilePath']}") FileUrlResource customersFile) {
        return new FlatFileItemReaderBuilder<Customer>().name("customersCompositeFileReader")
                .resource(customersFile)
                .lineMapper(lineMapper())
                .build();
    }

    @Bean
    public Job processAccounts(Step importTransactions, Step applyTransactions, Step generateSummary, Step importCustomers) {
        return jobBuilderFactory.get("processData3")
                .incrementer(new RunIdIncrementer())
        //        .start(importTransactions)
        //        .next(applyTransactions)
        //        .next(generateSummary)
                .start(importCustomers)
                .build();
    }

}
