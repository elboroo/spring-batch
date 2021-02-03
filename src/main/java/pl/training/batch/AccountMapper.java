package pl.training.batch;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class AccountMapper implements RowMapper<Account> {

    public static final String ACCOUNT_NUMBER = "account_number";
    public static final String BALANCE = "balance";

    @Override
    public Account mapRow(ResultSet resultSet, int i) throws SQLException {
        var account = new Account();
        account.setNumber(resultSet.getString(ACCOUNT_NUMBER));
        account.setBalance(resultSet.getBigDecimal(BALANCE));
        return account;
    }

}
