package pl.training.batch;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.IncorrectTokenCountException;

@Log
@RequiredArgsConstructor
public class SafeTransactionReader implements ItemStreamReader<Transaction> {

    private final ItemStreamReader<FieldSet> streamReader;
    private final TransactionMapper transactionMapper = new TransactionMapper();

    private int expectedRowsCount;
    private int counter;

    @Override
    public Transaction read() throws Exception {
        Transaction transaction = null;
        var fieldSet = streamReader.read();
        if (fieldSet != null) {
            if (fieldSet.getFieldCount() > 1) {
                transaction = transactionMapper.mapFieldSetByIndex(fieldSet);
                counter++;
            } else {
                expectedRowsCount = fieldSet.readInt(0);
                log.info("## " + expectedRowsCount);
            }
        }
        return transaction;
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
