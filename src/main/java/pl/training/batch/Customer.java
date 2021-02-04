package pl.training.batch;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class Customer {

    private String firstName;
    private String lastName;
    private String address;
    private List<Transaction> transactions = new ArrayList<>();

    public void addTransaction(Transaction transaction) {
        transactions.add(transaction);
    }

}
