package pl.training.batch;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.util.List;

public interface TransactionsRepository extends JpaRepository<Transaction, Long> {

    Transaction findByNumber(String number);

    @Query("select t from Transaction t where t.amount > :amount")
    List<Transaction> getAllBigTransaction(@Param("amount") BigDecimal amount);

}
