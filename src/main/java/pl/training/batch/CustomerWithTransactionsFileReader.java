package pl.training.batch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.*;

@RequiredArgsConstructor
public class CustomerWithTransactionsFileReader implements ItemStreamReader<Customer> {

    private final ItemStreamReader<Object> delegate;

    private Object lastRecord = null;

    @Override
    public Customer read() throws Exception {
        Object record = null;
        Customer customer = null;
        if (lastRecord == null) {
            record = delegate.read();
        }
        if (record instanceof Customer) {
            customer = (Customer) record;
            while (readNext() instanceof Transaction) {
                customer.addTransaction((Transaction) lastRecord);
                lastRecord = null;
            }
        }
        return customer;
    }

    private Object readNext() throws Exception {
        if (lastRecord == null) {
            lastRecord = delegate.read();
        }
        return lastRecord;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        delegate.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        delegate.update(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
        delegate.close();
    }
}
