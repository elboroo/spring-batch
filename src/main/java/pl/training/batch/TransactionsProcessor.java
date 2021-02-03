package pl.training.batch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.ItemProcessor;

@RequiredArgsConstructor
public class TransactionsProcessor implements ItemProcessor<Account, Account> {

    private final TransactionsRepository transactionsRepository;

    @Override
    public Account process(Account account) {
        transactionsRepository.findByNumber(account.getNumber())
                .forEach(transaction -> account.changeBalanceBy(transaction.getAmount()));
        return account;
    }

}
