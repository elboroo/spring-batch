package pl.training.batch;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class TransactionMapper implements FieldSetMapper<Transaction> {

    public static final String ACCOUNT_NUMBER = "accountNumber";
    public static final String TIMESTAMP = "timestamp";
    public static final String AMOUNT = "amount";
    public static final String DATE_FORMAT = "yyyy-MM-DD HH:mm:ss";

    @Override
    public Transaction mapFieldSet(FieldSet fieldSet) throws BindException {
        var transaction = new Transaction();
        transaction.setNumber(fieldSet.readString(ACCOUNT_NUMBER));
        transaction.setTimestamp(fieldSet.readDate(TIMESTAMP, DATE_FORMAT));
        transaction.setAmount(fieldSet.readBigDecimal(AMOUNT));
        return transaction;
    }

}
