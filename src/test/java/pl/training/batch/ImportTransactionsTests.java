package pl.training.batch;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBatchTest
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = BatchApplication.class)
class ImportTransactionsTests {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private TransactionsRepository transactionsRepository;

    @Test
    void shouldImportTransactions() throws Exception {
        var parameters = new JobParametersBuilder()
                .addString("transactionsFilePath", "/Users/lukas/Desktop/batch/src/main/resources/transactions.csv")
                .addDate("timestamp", new Date())
                .toJobParameters();
        var jobExecution = jobLauncherTestUtils.launchStep("importTransactions", parameters);
        assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
        assertEquals(4, transactionsRepository.count());
    }

    @AfterEach
    void clean() {
        transactionsRepository.deleteAll();
    }

}
