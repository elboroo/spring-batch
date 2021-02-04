package pl.training.batch;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.IncorrectTokenCountException;

@Log
@RequiredArgsConstructor
public class SafeTransactionReader implements ItemStreamReader<Transaction> {

    private final ItemStreamReader<FieldSet> streamReader;
    private final TransactionMapper transactionMapper = new TransactionMapper();

    private StepExecution stepExecution;
    private int counter; // should be updated after restart

    @Override
    public Transaction read() throws Exception {
        /*if (counter > 2) {
            throw  new RuntimeException();
        }*/
        Transaction transaction = null;
        var fieldSet = streamReader.read();
        if (fieldSet != null) {
            if (fieldSet.getFieldCount() > 1) {
                transaction = transactionMapper.mapFieldSetByIndex(fieldSet);
                counter++;
            } else {
                if (counter != fieldSet.readInt(0)) {
                    stepExecution.setTerminateOnly();
                }
            }
        }
        return transaction;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        streamReader.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        streamReader.update(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
        streamReader.close();
    }

}
