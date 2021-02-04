package pl.training.batch;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface CustomerNamesRepository extends JpaRepository<CustomerName, Long> {

    Optional<CustomerName> findByName(String name);

}
