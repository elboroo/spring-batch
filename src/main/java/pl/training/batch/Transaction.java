package pl.training.batch;

import lombok.Data;

import javax.persistence.*;
import java.math.BigDecimal;
import java.util.Objects;

@Table(name = "transactions")
@Entity
@Data
public class Transaction {

    @GeneratedValue
    @Id
    private Long id;
    @Column(name = "account_number")
    private String number;
    private BigDecimal amount;

    @Override
    public boolean equals(Object otherObject) {
        if (this == otherObject) {
            return true;
        }
        if (!(otherObject instanceof Transaction)) {
            return false;
        }
        var transaction = (Transaction) otherObject;
        return Objects.equals(id, transaction.id);
    }

    @Override
    public int hashCode() {
        return 13;
    }

}
