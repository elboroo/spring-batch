package pl.training.batch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.ItemProcessor;

@RequiredArgsConstructor
public class TransactionsProcessor implements ItemProcessor<Account, Account> {

    private final TransactionsRepository transactionsRepository;

    @Override
    public Account process(Account account) {
        var transactions = transactionsRepository.findByNumber(account.getNumber());
        if (transactions.isEmpty()) {
            throw new IllegalStateException();
        }
        transactions.forEach(transaction -> account.changeBalanceBy(transaction.getAmount()));
        return account;
    }

}
