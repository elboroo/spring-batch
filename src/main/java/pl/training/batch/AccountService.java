package pl.training.batch;

import lombok.extern.java.Log;
import org.springframework.batch.core.ExitStatus;
import org.springframework.stereotype.Service;

@Service
@Log
public class AccountService {

    public void incrementBalance(long amount) {
        log.info("Incrementing balance");
        // return ExitStatus.COMPLETED
    }

}
