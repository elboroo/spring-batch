package pl.training.batch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TransactionsProcessorTests {

    private static final String ACCOUNT_NUMBER = "1";
    private static final BigDecimal ACCOUNT_BALANCE = BigDecimal.ZERO;
    private static final BigDecimal TRANSACTION_AMOUNT = BigDecimal.TEN;

    private final TransactionsRepository transactionsRepository = mock(TransactionsRepository.class);
    private final TransactionsProcessor transactionsProcessor = new TransactionsProcessor(transactionsRepository);
    private final Account account = new Account();
    private final Transaction transaction = new Transaction();

    @BeforeEach
    void init() {
        account.setNumber(ACCOUNT_NUMBER);
        account.setBalance(ACCOUNT_BALANCE);
        transaction.setAmount(TRANSACTION_AMOUNT);
    }

    @Test
    void shouldThrowExceptionWhenAccountNotExists() {
        when(transactionsRepository.findByNumber(ACCOUNT_NUMBER)).thenReturn(emptyList());
        assertThrows(IllegalStateException.class, () -> transactionsProcessor.process(account));
    }

    @Test
    void shouldApplyTransactions() {
        when(transactionsRepository.findByNumber(ACCOUNT_NUMBER)).thenReturn(List.of(transaction));
        var initialBalance = account.getBalance();
        assertEquals(initialBalance.add(TRANSACTION_AMOUNT), transactionsProcessor.process(account).getBalance());
    }

}
