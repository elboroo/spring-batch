package pl.training.batch;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;

public class FileNameValidator implements JobParametersValidator {

    private static final String FILE_NAME = "fileName";

    @Override
    public void validate(JobParameters jobParameters) throws JobParametersInvalidException {
        var fileName = jobParameters.getString(FILE_NAME);
        if (fileName == null || fileName.length() < 3) {
            throw new JobParametersInvalidException("File name is invalid");
        }
    }

}
