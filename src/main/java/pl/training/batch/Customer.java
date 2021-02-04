package pl.training.batch;

import lombok.Data;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Pattern;
import java.util.ArrayList;
import java.util.List;

@Data
public class Customer {

    @NotEmpty
    private String firstName;
    @Pattern(regexp = "[a-zA-Z]+")
    private String lastName;
    private String address;
    private List<Transaction> transactions = new ArrayList<>();

    public void addTransaction(Transaction transaction) {
        transactions.add(transaction);
    }

}
