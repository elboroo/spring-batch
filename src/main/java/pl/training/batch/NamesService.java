package pl.training.batch;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.List;

@Log
@Service
@RequiredArgsConstructor
public class NamesService {

    private final CustomerNamesRepository customerNamesRepository;

    public List<CustomerName> findAll() {
        return customerNamesRepository.findAll();
    }

    @CacheEvict(cacheNames = "names", allEntries = true)
    public void invalidate() {

    }

    @Cacheable(cacheNames = "names")
    public boolean hasName(String name) {
        log.info("Reading from database...");
        return customerNamesRepository.findByName(name).isPresent();
    }

}
