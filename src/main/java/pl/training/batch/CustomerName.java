package pl.training.batch;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Table(name = "customer_names")
@Entity
@RequiredArgsConstructor
@NoArgsConstructor
public class CustomerName {

    @GeneratedValue
    @Id
    private Long id;
    @NonNull
    public String name;

}
