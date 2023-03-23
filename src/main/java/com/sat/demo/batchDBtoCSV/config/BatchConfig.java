package com.sat.demo.batchDBtoCSV.config;

import com.sat.demo.batchDBtoCSV.entity.Customer;
import com.sat.demo.batchDBtoCSV.processor.CustomerProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

@Configuration
@EnableBatchProcessing
public class BatchConfig {
    @Value("${filetype}")
    private String filetype;
    @Autowired
    public JobBuilderFactory jobBuilderFactory;
    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;

    @Bean
    public DataSource dataSource(){
        final DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://localhost:3306/customerdb1?useSSL=false");
        dataSource.setUsername("root");
        dataSource.setPassword("root");
        return dataSource;
    }

    @Bean
    public JdbcCursorItemReader<Customer> reader(){
        JdbcCursorItemReader<Customer> reader = new JdbcCursorItemReader<Customer>();
        reader.setDataSource(dataSource());
        reader.setSql("select customer_id,contact,country,dob,email,first_name,last_name,gender from customer_info");
        reader.setRowMapper(new CustomerRowMapper());
        return reader;
    }

    public class CustomerRowMapper implements RowMapper<Customer> {
        public Customer mapRow(ResultSet rs,int rowNum)throws SQLException{
            Customer customer = new Customer();
            customer.setId(rs.getInt("customer_id"));
            customer.setFirstName(rs.getString("first_name"));
            customer.setLastName(rs.getString("last_name"));
            customer.setEmail(rs.getString("email"));
            customer.setGender(rs.getString("gender"));
            customer.setContactNo(rs.getString("contact"));
            customer.setCountry(rs.getString("country"));
            customer.setDob(rs.getString("dob"));
            return customer;
        }
    }
    @Bean
    public CustomerProcessor processor(){
        return new CustomerProcessor();
    }

    @Bean
    public FlatFileItemWriter<Customer> writer(){
        FlatFileItemWriter<Customer> writer = new FlatFileItemWriter<Customer>();
        writer.setLineSeparator("\n");
        writer.setResource(new FileSystemResource("D://work/result_customer."+filetype));
//        writer.setResource(new ClassPathResource("result_customer.csv"));
        writer.setLineAggregator(new DelimitedLineAggregator<Customer>()
        {
            {
            setDelimiter(",");
            setFieldExtractor(new BeanWrapperFieldExtractor<Customer>(){
                {
                setNames(new String[]{"id","firstName","LastName","email","gender","contactNo","country","dob"});
                }
            });
            }
        });
        return writer;
    }

    @Bean
    public Step step1(){
        return stepBuilderFactory.get("step1").<Customer,Customer> chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }

    @Bean
    public Job exportCustomerJob(){
        return jobBuilderFactory.get("exportUserJob")
                .incrementer(new RunIdIncrementer())
                .flow(step1())
                .end()
                .build();
    }
}
