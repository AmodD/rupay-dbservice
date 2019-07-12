package com.fortiate.rupaydbservice;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.r2dbc.repository.query.Query;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;


import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@SpringBootApplication
@Slf4j
public class Application implements ApplicationRunner {
	
	private static Logger logger = LoggerFactory.getLogger(Application.class);

    @Autowired
    CustomerRepository repo;
    
    @Autowired
    TransactionRepository txnrepo;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        logger.info("Seeding data!");
        Flux<String> names = Flux.just("newraj", "newdavid", "newpam").delayElements(Duration.ofSeconds(1));
        Flux<Integer> ages = Flux.just(25, 27, 30).delayElements(Duration.ofSeconds(1));
        Flux<String> description = Flux.just("indian", "", "").delayElements(Duration.ofSeconds(1));
    
        Flux<String> txn = Flux.just("2008238283428382hjfdfdfgd8gdhfdfgd99s0AHUH**U*SSBHBSD&D(S))Ggygsa7d666238232", "2008232udfhs7yfshdfs7df7shdf778328y293r").delayElements(Duration.ofSeconds(1));
        Flux<String> mscores = Flux.just("80", "62").delayElements(Duration.ofSeconds(1));
        Flux<Integer> chpids = Flux.just(77728,22342).delayElements(Duration.ofSeconds(1));
    	
        logger.info("Reached here...");

	Flux<Customer> customers = Flux.zip(names, ages,description).map(tupple -> {
		return new Customer(null, tupple.getT1(), tupple.getT2(), tupple.getT3());
    	});
	
	Flux<Transaction> transactions = Flux.zip(txn, mscores, chpids).map(tupple -> {
            return new Transaction(null, tupple.getT1(), tupple.getT2(), tupple.getT3());
    	});
    
    	repo.deleteAll()
                .thenMany(customers.flatMap(c -> repo.save(c))
                        .thenMany(repo.findAll())
                ).subscribe(System.out::println);
    	
	txnrepo.deleteAll()
                .thenMany(transactions.flatMap(t -> txnrepo.save(t))
                        .thenMany(txnrepo.findAll())
                ).subscribe(System.out::println);
	
    logger.info("...end....");

    }
}



@Configuration
@EnableR2dbcRepositories
class DatabaseConfiguration extends AbstractR2dbcConfiguration {

    @Bean
    public ConnectionFactory connectionFactory() {
        return new PostgresqlConnectionFactory(
                PostgresqlConnectionConfiguration.builder()
                        .host("localhost")
                        .port(5432)
                        .database("demodb")
                        .username("demouser")
                        .password("demopwd")
                        .build()
        );
    }

}

interface CustomerRepository extends ReactiveCrudRepository<Customer, Long> {
    @Query("select * from customer where name = $1 and age = $2")
    Flux<Customer> findByNameAndAge(String name, Integer age);
}

interface TransactionRepository extends ReactiveCrudRepository<Transaction, Long> {
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@Table("customer")
class Customer {

	public Customer(Object object, String t1, Integer t2, String t3) {
		// TODO Auto-generated constructor stub
		
		this.name = t1;
		this.age = t2;
		this.description = t3;
		
	}
	@Id
    public Long id;
    public String name;
    public Integer age;
    public String description;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@Table("transaction")
class Transaction {
    public Transaction(Object object, String t1, String t2, Integer t3) {
    	this.message = t1;
    	this.mscore =t2;
    	this.chpid = t3;
	}
	@Id
    public Long id;
    public String message;
    public String mscore;
    public Integer chpid; // card holder profile
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@Table("field")
class Field {
    @Id
    public Integer id;
    public String element;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@Table("fielddata")
class FieldData {
    @Id
    public Integer id;
    public Long transaction_id;
    public Integer field_id;
    public String value;
}

/*
CREATE TABLE customer (
   id  SERIAL PRIMARY KEY,
   name VARCHAR(50) NOT NULL,
   AGE INT NOT NULL
);

CREATE TABLE transaction ( id  SERIAL PRIMARY KEY, message TEXT NOT NULL,mscore VARCHAR,chpid INT NOT NULL);




 */
